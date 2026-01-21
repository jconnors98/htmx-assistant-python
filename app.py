import os
import re
import io
import json
import secrets
import logging
import hmac
from functools import wraps
from flask import Flask, Blueprint, request, send_from_directory, Response, url_for, send_file
from werkzeug.datastructures import FileStorage
from markdown import markdown
import bleach
from decouple import config
from openai import OpenAI
from pymongo import MongoClient
from pymongo import ReturnDocument
from pymongo.server_api import ServerApi
from bson import ObjectId
import boto3
import requests
from jose import jwk, jwt
from datetime import datetime, timedelta
import hashlib
import threading
from conversation_service import ConversationService
from scraping_service import ScrapingService
from scrape_scheduler import ScrapeScheduler
from assistant_services import ScraperClient
from packages.common.scraper_contracts import ScraperQueueConfig
from document_intelligence_service import DocumentIntelligenceService
from tools import DocumentToolbox
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from functions import (
    _get_priority_source, _get_jwks, _is_super_admin, _async_log_prompt,
    _parse_date, _normalize_color, _normalize_text_color, _process_natural_language_query,
    _search_prompts_tool, _get_unique_prompts_data, _search_permits_tool, _get_analytics_data_for_query
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not config("OPENAI_API_KEY"):
    raise RuntimeError("Missing API key. Check your .env file.")

client = OpenAI(api_key=config("OPENAI_API_KEY"))
VECTOR_STORE_ID = config("OPENAI_VECTOR_STORE_ID", default=None)

mongo_client = MongoClient(config("MONGO_URI"), server_api=ServerApi("1"))
try:
    mongo_client.admin.command("ping")
    print("MongoDB connection successful.")

except Exception as e:
    raise RuntimeError(f"Failed to connect to MongoDB: {e}")

db = mongo_client.get_database(config("MONGO_DB", default="bcca-assistant"))
modes_collection = db.get_collection("modes")
documents_collection = db.get_collection("documents")
prompt_logs_collection = db.get_collection("prompt_logs")
superadmins_collection = db.get_collection("superadmins")
reset_tokens_collection = db.get_collection("password_reset_tokens")
scraped_content_collection = db.get_collection("scraped_content")
scraping_jobs_collection = db.get_collection("scraping_jobs")
document_projects_collection = db.get_collection("document_projects")
blocked_pages_collection = db.get_collection("blocked_pages")
api_tokens_collection = db.get_collection("api_tokens")

localDevMode = config("LOCAL_DEV_MODE", default="false").lower()

if not localDevMode == "true" and config("SCRAPER_ENVIRONMENT", default="dev") == "prod":
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/bitnami/playwright-browsers")
    os.environ.setdefault("XDG_CACHE_HOME", "/opt/bitnami/playwright-cache")

# elif localDevMode == "false" and config("SCRAPER_ENVIRONMENT", default="dev") == "dev":
#     os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/projects/pw-browsers")

AWS_ACCESS_KEY_ID = config("AWS_ACCESS_KEY_ID", default=None)
AWS_SECRET_ACCESS_KEY = config("AWS_SECRET_ACCESS_KEY", default=None)
COGNITO_REGION = config("COGNITO_REGION", default=None)
COGNITO_USER_POOL_ID = config("COGNITO_USER_POOL_ID", default=None)
COGNITO_APP_CLIENT_ID = config("COGNITO_APP_CLIENT_ID", default=None)
SES_SENDER_EMAIL = config("SES_SENDER_EMAIL", default=None)

DEFAULT_MODE_COLOR = "#82002d"
DEFAULT_TEXT_COLOR = "#ffffff"

DOC_INTEL_ENABLED = config("DOC_INTEL_ENABLED", default="false").lower() == "true"
DOC_INTEL_STORAGE_DIR = config(
    "DOC_INTEL_STORAGE_DIR",
    default=os.path.join(os.getcwd(), "storage", "doc_intel"),
)
DOC_INTEL_DEFAULT_SETTINGS = {
    "enabled": False,
    "auto_ingest": False,
    "storage_prefix": "doc_intel",
}
DOC_INTEL_MAX_UPLOAD_FILES = int(config("DOC_INTEL_MAX_UPLOAD_FILES", default="20"))
DOC_INTEL_MAX_UPLOAD_MB = int(config("DOC_INTEL_MAX_UPLOAD_MB", default="200"))
DOC_INTEL_ALLOWED_EXTENSIONS = set(
    ext.strip().lower()
    for ext in config(
        "DOC_INTEL_ALLOWED_EXTENSIONS",
        default="pdf,zip,doc,docx,xls,xlsx,ppt,pptx,txt,csv,jpg,jpeg,png,tif,tiff",
    ).split(",")
    if ext.strip()
)
DOC_INTEL_EXPIRY_MINUTES = int(config("DOC_INTEL_EXPIRY_MINUTES", default="30"))

def _log_access_denied(action: str, *, mode_id=None, content_id=None, mode_owner_id=None, content_owner_id=None, extra=None):
    """Log consistent auth debug info to help diagnose 403s (especially mixed user_id types in Mongo)."""
    try:
        req_user = getattr(request, "user", {}) or {}
        req_sub = req_user.get("sub")
        is_super = bool(req_user.get("is_super_admin"))
        logger.warning(
            "ACCESS_DENIED action=%s req_sub=%s req_is_super=%s mode_id=%s content_id=%s mode_owner_id=%s mode_owner_type=%s content_owner_id=%s content_owner_type=%s extra=%s",
            action,
            str(req_sub),
            is_super,
            str(mode_id) if mode_id is not None else None,
            str(content_id) if content_id is not None else None,
            str(mode_owner_id) if mode_owner_id is not None else None,
            type(mode_owner_id).__name__ if mode_owner_id is not None else None,
            str(content_owner_id) if content_owner_id is not None else None,
            type(content_owner_id).__name__ if content_owner_id is not None else None,
            extra,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("ACCESS_DENIED_LOG_FAILED action=%s error=%s", action, str(exc))


def _merge_doc_intel_settings(settings):
    merged = {**DOC_INTEL_DEFAULT_SETTINGS}
    if isinstance(settings, dict):
        merged.update(settings)
    merged["enabled"] = DOC_INTEL_ENABLED and bool(merged.get("enabled"))
    return merged


def _attach_doc_intel_metadata(doc):
    doc["doc_intelligence_enabled"] = DOC_INTEL_ENABLED and bool(doc.get("doc_intelligence_enabled"))
    doc["doc_intelligence_settings"] = _merge_doc_intel_settings(doc.get("doc_intelligence_settings"))
    return doc


def _doc_intel_mode_lookup(mode_name: str):
    if not mode_name:
        return None, ({"error": "mode is required"}, 400)
    mode_doc = modes_collection.find_one({"name": mode_name})
    if not mode_doc:
        return None, ({"error": "Mode not found"}, 404)
    if not (DOC_INTEL_ENABLED and mode_doc.get("doc_intelligence_enabled")):
        return None, ({"error": "Document intelligence is not enabled for this mode"}, 403)
    return mode_doc, None


def _doc_intel_validate_files(files):
    if len(files) > DOC_INTEL_MAX_UPLOAD_FILES:
        return {"error": f"Maximum {DOC_INTEL_MAX_UPLOAD_FILES} files per upload"}, 400
    total_bytes = request.content_length or 0
    if total_bytes and total_bytes > DOC_INTEL_MAX_UPLOAD_MB * 1024 * 1024:
        return {"error": f"Upload exceeds {DOC_INTEL_MAX_UPLOAD_MB}MB limit"}, 400
    for file in files:
        filename = (file.filename or "").lower()
        ext = filename.rsplit(".", 1)[-1] if "." in filename else ""
        if ext not in DOC_INTEL_ALLOWED_EXTENSIONS:
            return {"error": f"File type not allowed: {filename}"}, 400
    return None



S3_BUCKET = config("S3_BUCKET", default="builders-copilot")
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=COGNITO_REGION
)
ses = boto3.client(
    "ses",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=COGNITO_REGION
)
_jwks = None

document_toolbox = DocumentToolbox(
    openai_client=client,
    storage_dir=DOC_INTEL_STORAGE_DIR,
)

doc_intelligence_service = DocumentIntelligenceService(
    modes_collection=modes_collection,
    projects_collection=document_projects_collection,
    storage_dir=DOC_INTEL_STORAGE_DIR,
    toolbox=document_toolbox,
    expiry_minutes=DOC_INTEL_EXPIRY_MINUTES,
) if DOC_INTEL_ENABLED else None

conversation_service = ConversationService(
    db,
    modes_collection,
    client,
    VECTOR_STORE_ID,
    doc_intelligence_service=doc_intelligence_service,
    document_intelligence_enabled=DOC_INTEL_ENABLED,
)

SCRAPER_EXECUTION_MODE = config("SCRAPER_EXECUTION_MODE", default="local").lower()
SCRAPER_ENVIRONMENT = config("SCRAPER_ENVIRONMENT", default="prod")
SCRAPER_SQS_QUEUE_URL = config("SCRAPER_SQS_QUEUE_URL", default=None)
SCRAPER_SQS_REGION = config("SCRAPER_SQS_REGION", default=COGNITO_REGION or "us-east-1")
SCRAPER_SQS_MESSAGE_GROUP_ID = config("SCRAPER_SQS_MESSAGE_GROUP_ID", default=None)

scraping_service = None
scraper_client = None

if SCRAPER_EXECUTION_MODE == "local":
    scraping_service = ScrapingService(
        client,
        db,
        VECTOR_STORE_ID,
    )
    scraper_client = ScraperClient(
        mode="local",
        jobs_collection=scraping_jobs_collection,
        scraper_environment="dev" if localDevMode == "true" else "prod",
        scraping_service=scraping_service,
    )
else:
    if not SCRAPER_SQS_QUEUE_URL:
        raise RuntimeError("SCRAPER_SQS_QUEUE_URL is required when SCRAPER_EXECUTION_MODE='remote'")

    sqs_client = boto3.client(
        "sqs",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=SCRAPER_SQS_REGION,
    )
    queue_config = ScraperQueueConfig(
        queue_url=SCRAPER_SQS_QUEUE_URL,
        region_name=SCRAPER_SQS_REGION,
        message_group_id=SCRAPER_SQS_MESSAGE_GROUP_ID or None,
    )
    scraper_client = ScraperClient(
        mode="remote",
        jobs_collection=scraping_jobs_collection,
        scraper_environment=SCRAPER_ENVIRONMENT,
        sqs_client=sqs_client,
        queue_config=queue_config,
    )

scrape_scheduler = ScrapeScheduler(
    modes_collection,
    scraping_jobs_collection,
    scraper_client=scraper_client,
    scraping_service=scraping_service,
    doc_intelligence_service=doc_intelligence_service,
)


def cognito_auth_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return {"error": "Unauthorized"}, 401
        token = auth.split(" ", 1)[1]
        try:
            headers = jwt.get_unverified_header(token)
        except Exception:
            return {"error": "Invalid token"}, 401
        jwks = _get_jwks()
        key = next((k for k in jwks if k.get("kid") == headers.get("kid")), None)
        if not key:
            return {"error": "Unauthorized, no key found"}, 401
        try:
            claims = jwt.decode(
                token,
                key,
                algorithms=[headers.get("alg")],
                audience=COGNITO_APP_CLIENT_ID,
            )
        except Exception:
            return {"error": "Unauthorized, could not decode token"}, 401
        user_id = claims.get("sub")
        request.user = {
            "sub": user_id,
            "is_super_admin": _is_super_admin(user_id, superadmins_collection),
        }
        return fn(*args, **kwargs)

    return wrapper


def _hash_api_token(token: str) -> str:
    """
    Hash an API token for storage/lookup using HMAC-SHA256.

    Why HMAC instead of plain sha256(token)?
    - Prevents offline guessing/rainbow tables if the DB is leaked.
    - Still allows direct lookup by hash (unlike per-token random salts).
    """
    secret = config("API_TOKEN_HMAC_SECRET", default=None)
    if not secret:
        raise RuntimeError("API_TOKEN_HMAC_SECRET is not configured")
    token_bytes = (token or "").encode("utf-8")
    secret_bytes = str(secret).encode("utf-8")
    return hmac.new(secret_bytes, token_bytes, hashlib.sha256).hexdigest()


def token_auth_required(fn):
    """
    Authenticate using an API token stored in MongoDB `api_tokens`.

    Header forms:
    - Authorization: Bearer <token>
    - X-API-Token: <token>

    Usage logging (best-effort; stored on the token doc):
    - usage_count (increment)
    - last_used_at / last_used_ip / last_used_user_agent / last_used_path / last_used_method
    - usage_log (rolling; last 100 entries)
    """

    @wraps(fn)
    def wrapper(*args, **kwargs):
        token = ""
        auth = request.headers.get("Authorization", "") or ""
        if auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()
        if not token:
            token = (request.headers.get("X-API-Token") or "").strip()

        if not token:
            return {"error": "Unauthorized"}, 401

        try:
            token_hash = _hash_api_token(token)
        except Exception as e:  # noqa: BLE001
            print(f"API token auth misconfigured: {e}")
            return {"error": "Token auth not configured"}, 500

        token_doc = api_tokens_collection.find_one({"token_hash": token_hash})
        if not token_doc:
            # Back-compat/migration path: if a legacy doc stored plaintext token, migrate it.
            legacy = api_tokens_collection.find_one({"token": token})
            if not legacy:
                return {"error": "Unauthorized"}, 401
            try:
                api_tokens_collection.update_one(
                    {"_id": legacy["_id"]},
                    {"$set": {"token_hash": token_hash}, "$unset": {"token": ""}},
                )
            except Exception as e:  # noqa: BLE001
                print(f"Failed to migrate legacy api token doc: {e}")
            token_doc = legacy
        if token_doc.get("active", True) is False:
            return {"error": "Unauthorized"}, 401

        # Usage logging (best-effort; do not block request)
        try:
            ip_addr = request.headers.get("X-Forwarded-For", request.remote_addr)
            user_agent = request.headers.get("User-Agent", "")
            log_entry = {
                "at": datetime.utcnow(),
                "ip": ip_addr,
                "ua": user_agent,
                "path": request.path,
                "method": request.method,
            }
            api_tokens_collection.update_one(
                {"_id": token_doc["_id"]},
                {
                    "$set": {
                        "last_used_at": log_entry["at"],
                        "last_used_ip": ip_addr,
                        "last_used_user_agent": user_agent,
                        "last_used_path": request.path,
                        "last_used_method": request.method,
                    },
                    "$inc": {"usage_count": 1},
                    "$push": {"usage_log": {"$each": [log_entry], "$slice": -100}},
                },
            )
        except Exception as e:  # noqa: BLE001
            print(f"Error logging api token usage: {e}")

        request.api_token = {
            "_id": str(token_doc.get("_id")),
            "name": token_doc.get("name"),
            "user_id": token_doc.get("user_id"),
        }

        return fn(*args, **kwargs)

    return wrapper


def _generate_password_reset_email_html(reset_link, token_expiry_minutes=15):
    """Generate HTML email template for password reset."""
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                line-height: 1.6;
                color: #333;
                background-color: #f5f5f5;
                margin: 0;
                padding: 0;
            }}
            .container {{
                max-width: 600px;
                margin: 40px auto;
                background-color: #ffffff;
                border-radius: 12px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                overflow: hidden;
            }}
            .header {{
                background: linear-gradient(135deg, #82002d 0%, #a0003f 100%);
                color: #ffffff;
                padding: 40px 20px;
                text-align: center;
            }}
            .header h1 {{
                margin: 0;
                font-size: 28px;
                font-weight: 600;
            }}
            .content {{
                padding: 40px 30px;
            }}
            .content p {{
                margin: 0 0 20px 0;
                font-size: 16px;
            }}
            .button {{
                display: inline-block;
                background: linear-gradient(135deg, #82002d 0%, #a0003f 100%);
                color: #ffffff !important;
                text-decoration: none;
                padding: 16px 32px;
                border-radius: 8px;
                font-weight: 600;
                font-size: 16px;
                margin: 20px 0;
                box-shadow: 0 4px 6px rgba(130, 0, 45, 0.2);
            }}
            .button:hover {{
                background: linear-gradient(135deg, #6b0024 0%, #82002d 100%);
            }}
            .footer {{
                background-color: #f8fafc;
                padding: 30px;
                text-align: center;
                font-size: 14px;
                color: #666;
                border-top: 1px solid #e2e8f0;
            }}
            .expiry-notice {{
                background-color: #fff8e1;
                border-left: 4px solid #ffc107;
                padding: 15px;
                margin: 20px 0;
                border-radius: 4px;
            }}
            .security-notice {{
                background-color: #e3f2fd;
                border-left: 4px solid #2196f3;
                padding: 15px;
                margin: 20px 0;
                border-radius: 4px;
            }}
            .link-alternative {{
                background-color: #f5f5f5;
                padding: 15px;
                border-radius: 8px;
                margin: 20px 0;
                word-break: break-all;
                font-size: 12px;
                color: #666;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🔐 Password Reset Request</h1>
            </div>
            <div class="content">
                <p>Hello,</p>
                <p>We received a request to reset your password. Click the button below to create a new password:</p>
                
                <center>
                    <a href="{reset_link}" class="button">Reset Your Password</a>
                </center>
                
                <div class="expiry-notice">
                    <strong>⏰ Important:</strong> This link will expire in {token_expiry_minutes} minutes for security reasons.
                </div>
                
                <div class="security-notice">
                    <strong>🛡️ Security Note:</strong> If you didn't request this password reset, you can safely ignore this email. Your account remains secure.
                </div>
                
                <p style="margin-top: 30px; font-size: 14px; color: #666;">
                    If the button above doesn't work, copy and paste this link into your browser:
                </p>
                <div class="link-alternative">
                    {reset_link}
                </div>
            </div>
            <div class="footer">
                <p>This is an automated message, please do not reply to this email.</p>
                <p>&copy; {datetime.now().year} Builder's Copilot. All rights reserved.</p>
            </div>
        </div>
    </body>
    </html>
    """
    return html_body


def _generate_password_reset_email_text(reset_link, token_expiry_minutes=15):
    """Generate plain text email for password reset."""
    text_body = f"""
Password Reset Request

We received a request to reset your password.

Click the link below to create a new password:
{reset_link}

This link will expire in {token_expiry_minutes} minutes for security reasons.

If you didn't request this password reset, you can safely ignore this email. Your account remains secure.

---
This is an automated message, please do not reply to this email.
© {datetime.now().year} Builder's Copilot. All rights reserved.
    """
    return text_body


def _send_password_reset_email(to_email, reset_link, token_expiry_minutes=15):
    """Send password reset email via AWS SES."""
    if not SES_SENDER_EMAIL:
        raise ValueError("SES_SENDER_EMAIL not configured")
    
    html_body = _generate_password_reset_email_html(reset_link, token_expiry_minutes)
    text_body = _generate_password_reset_email_text(reset_link, token_expiry_minutes)
    
    response = ses.send_email(
        Source=SES_SENDER_EMAIL,
        Destination={
            'ToAddresses': [to_email]
        },
        Message={
            'Subject': {
                'Data': 'Password Reset Request',
                'Charset': 'UTF-8'
            },
            'Body': {
                'Text': {
                    'Data': text_body,
                    'Charset': 'UTF-8'
                },
                'Html': {
                    'Data': html_body,
                    'Charset': 'UTF-8'
                }
            }
        }
    )
    
    return response


routes = Blueprint("routes", __name__, static_folder='public', static_url_path='')

@routes.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Content-Security-Policy"] = "frame-ancestors *"
    return response

# @auth_bp.after_request
# def add_security_headers_auth(response):
#     response.headers["X-Content-Type-Options"] = "nosniff"
#     response.headers["Referrer-Policy"] = "no-referrer"
#     response.headers["Content-Security-Policy"] = "frame-ancestors *"
#     return response

@routes.post("/upload-files")
def upload_files():
    files = request.files.getlist("files")
    mode_name = (request.form.get("mode") or "").strip()
    doc_intel_session_id = (request.form.get("doc_intel_session_id") or "").strip()
    upload_to_openai_flag = (request.form.get("upload_to_openai") or "").lower() == "true"
    
    logger.info(f"Upload request received: {len(files)} files, mode={mode_name}, doc_intel_session={doc_intel_session_id}")
    
    mode_doc = None
    if mode_name:
        mode_doc = modes_collection.find_one({"name": mode_name})
    file_ids = []
    doc_intel_candidates = []
    doc_intel_active = (
        doc_intelligence_service
        and mode_doc
        and mode_doc.get("doc_intelligence_enabled")
        and DOC_INTEL_ENABLED
        and doc_intel_session_id
    )

    for file in files:
        if file and file.filename:
            logger.info(f"Processing upload: {file.filename}")
            data = file.read()
            file_stream = io.BytesIO(data)
            # Only upload to OpenAI when not in doc-intel-only flow, or when explicitly requested.
            if (not doc_intel_active) or upload_to_openai_flag:
                uploaded = client.files.create(
                    file=(file.filename, file_stream), purpose="assistants"
                )
                file_ids.append(uploaded.id)
                logger.info(f"Uploaded to OpenAI: {uploaded.id}")
            else:
                logger.info("Skipped OpenAI upload (doc-intel flow)")
            
            if doc_intel_active:
                doc_intel_candidates.append(
                    FileStorage(
                        stream=io.BytesIO(data),
                        filename=file.filename,
                        content_type=getattr(file, "content_type", None),
                    )
                )

    if doc_intel_candidates and mode_doc and doc_intel_session_id:
        try:
            logger.info(f"Handoff to DocIntel: {len(doc_intel_candidates)} candidates")
            doc_intelligence_service.ingest_files(mode_doc, doc_intel_session_id, doc_intel_candidates)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Doc intelligence ingest failed: {exc}", exc_info=True)
            print(f"Doc intelligence ingest failed: {exc}")
    
    return {"file_ids": file_ids}


@routes.post("/clear-conversation")
def clear_conversation():
    conversation_id = (request.form.get("conversation_id") or "").strip()
    if conversation_id:
        # Get and clear files from conversation
        file_ids = conversation_service.clear_conversation_files(conversation_id)
        
        # Delete the files from OpenAI
        for file_id in file_ids:
            try:
                client.files.delete(file_id)
                print(f"Deleted file: {file_id}")
            except Exception as e:  # noqa: BLE001
                print(f"Failed to delete file {file_id}: {e}")
    
    return {"status": "cleared"}


@routes.post("/ask")
def ask():
    message = (request.form.get("message") or "").strip()
    mode = (request.form.get("mode") or "").strip()
    tag = (request.form.get("tag") or "").strip()
    conversation_id = (request.form.get("conversation_id") or "").strip()
    previous_response_id = (request.form.get("response_id") or "").strip()
    doc_intel_session_id = (request.form.get("doc_intel_session_id") or "").strip()
    mode_doc = modes_collection.find_one({"name": mode}) if mode else None
    allow_file_upload = mode_doc.get("allow_file_upload", False) if mode_doc else False
    
    # Handle uploaded file IDs from the upload endpoint
    uploaded_file_ids = request.form.getlist("uploaded_files")
    openai_file_ids = uploaded_file_ids if allow_file_upload and uploaded_file_ids else []

    if not message:
        return (
            '<div class="chat-entry assistant">'
            '<div class="bubble">⚠️ Message is required.</div>'
            "</div>"
        )
    
    if not conversation_id:
        conversation_id = str(ObjectId())
    
    ip_addr = request.headers.get("X-Forwarded-For", request.remote_addr)
    print(f"Prompt sent from IP: {ip_addr}")
    
    # Log the user prompt
    threading.Thread(
        target=_async_log_prompt,
        kwargs={
            "prompt": message,
            "mode": mode_doc["_id"] if mode_doc else None,
            "ip_addr": ip_addr,
            "conversation_id": conversation_id,
            "prompt_logs_collection": prompt_logs_collection,
        },
    ).start()

    try:
        user_id = getattr(request, "user", {}).get("sub", "anonymous")
        gpt_text, response_id, _usage = conversation_service.respond(
            conversation_id=conversation_id,
            user_id=user_id,
            text=message,
            mode=mode,
            tag=tag,
            previous_response_id=previous_response_id or None,
            file_ids=openai_file_ids,
            doc_intel_session_id=doc_intel_session_id or None,
        )

        # Log the AI response
        threading.Thread(
            target=_async_log_prompt,
            kwargs={
                "response": gpt_text,
                "mode": mode_doc["_id"] if mode_doc else None,
                "ip_addr": ip_addr,
                "conversation_id": conversation_id,
                "response_id": response_id,
                "prompt_logs_collection": prompt_logs_collection,
            },
        ).start()

        # Store uploaded files with the conversation for future use
        if openai_file_ids:
            conversation_service.store_conversation_files(conversation_id, openai_file_ids)


        html_reply = markdown(gpt_text)

        # Auto-link plain URLs
        def _linkify(match):
            url = match.group(0)
            return f'<a href="{url}" target="_blank" rel="noopener">{url}</a>'

        html_reply = re.sub(r'(?<!href=")(https?://[^\s<]+)', _linkify, html_reply)

        html_reply = bleach.clean(
            html_reply,
            tags=list(bleach.sanitizer.ALLOWED_TAGS) + ["img", "p", "h3", "br"],
            attributes={"a": ["href", "target", "rel"], "img": ["src", "alt"]},
        )

        html = (
            '<div class="chat-entry assistant">'
            '<div class="bubble markdown">'
            f"{html_reply}"
            "</div></div>"
            f'<input type="hidden" id="conversation_id" name="conversation_id" value="{conversation_id}" hx-swap-oob="true"/>'
            f'<input type="hidden" id="response_id" name="response_id" value="{response_id}" hx-swap-oob="true"/>'
        )

        return html
    except Exception as err:  # noqa: BLE001
        print("Error getting AI response:", err)
        return (
            '<div class="chat-entry assistant">'
            '<div class="bubble">❌ There was an error getting a response. Please try again.</div>'
            "</div>"
        )


@routes.get("/modes")
def list_modes():
    docs = list(modes_collection.find({}, {"_id": 0, "database": 0}))
    for doc in docs:
        doc = _attach_doc_intel_metadata(doc)
        doc["color"] = _normalize_color(doc.get("color"), DEFAULT_MODE_COLOR)
        doc["text_color"] = _normalize_text_color(doc.get("text_color"), DEFAULT_TEXT_COLOR)
    return {"modes": docs}


@routes.get("/modes/<mode>")
def get_mode(mode):
    doc = modes_collection.find_one({"name": mode}, {"_id": 0, "database": 0})
    if not doc:
        return {"prompts": []}, 404
    doc = _attach_doc_intel_metadata(doc)
    return {
        "prompts": doc.get("prompts", []),
        "description": doc.get("description", ""),
        "intro": doc.get("intro", ""),
        "title": doc.get("title", ""),
        "allow_file_upload": doc.get("allow_file_upload", False),
        "disable_widget_on_mobile": bool(doc.get("disable_widget_on_mobile", False)),
        "has_files": doc.get("has_files", False),
        "color": _normalize_color(doc.get("color"), DEFAULT_MODE_COLOR),
        "text_color": _normalize_text_color(doc.get("text_color"), DEFAULT_TEXT_COLOR),
        "doc_intelligence_enabled": doc.get("doc_intelligence_enabled", False),
        "doc_intelligence_settings": doc.get("doc_intelligence_settings", {}),
    }


@routes.get("/admin/modes")
@cognito_auth_required
def list_modes_admin():
    docs = []
    print("Listing modes for user:", request.user["sub"])
    if request.user.get("is_super_admin"):
        cursor = modes_collection.find({})
    else:
        # String-safe match on user_id (avoid false negatives from mixed stored types).
        cursor = modes_collection.aggregate([
            {"$match": {"$expr": {"$eq": [{"$toString": "$user_id"}, str(request.user.get("sub"))]}}}
        ])
    for d in cursor:
        d["_id"] = str(d["_id"])
        d.pop("user_id", None)
        d["priority_source"] = _get_priority_source(d)
        d.pop("prioritize_files", None)
        d = _attach_doc_intel_metadata(d)
        d["color"] = _normalize_color(d.get("color"), DEFAULT_MODE_COLOR)
        d["text_color"] = _normalize_text_color(d.get("text_color"), DEFAULT_TEXT_COLOR)
        docs.append(d)
    return {"modes": docs}


@routes.get("/admin/modes/<mode_id>")
@cognito_auth_required
def get_mode_admin(mode_id):
    try:
        mode_obj_id = ObjectId(mode_id)
    except Exception:  # noqa: BLE001
        return {"error": "Invalid mode id"}, 400

    doc = modes_collection.find_one({"_id": mode_obj_id})
    if not doc:
        return {"error": "Not found"}, 404
    if not request.user.get("is_super_admin"):
        if not doc.get("user_id") or str(doc.get("user_id")) != str(request.user.get("sub")):
            return {"error": "Access denied"}, 403
    doc["_id"] = str(doc["_id"])
    doc["priority_source"] = _get_priority_source(doc)
    doc.pop("prioritize_files", None)
    doc.pop("user_id", None)
    doc = _attach_doc_intel_metadata(doc)
    doc["color"] = _normalize_color(doc.get("color"), DEFAULT_MODE_COLOR)
    doc["text_color"] = _normalize_text_color(doc.get("text_color"), DEFAULT_TEXT_COLOR)
    return doc


@routes.post("/admin/modes")
@cognito_auth_required
def create_mode():
    data = request.get_json() or {}
    name = (data.get("name") or "").strip()
    if not name:
        return {"error": "Name is required"}, 400
    if modes_collection.find_one({"name": name, "user_id": request.user["sub"]}):
        return {"error": "Mode already exists"}, 400
    doc = {
        "name": name,
        "user_id": request.user["sub"],
        "description": data.get("description", ""),
        "intro": data.get("intro", ""),
        "title": data.get("title", ""),
        "prompts": data.get("prompts", []),
        "tags": data.get("tags", []),
        "preferred_sites": data.get("preferred_sites", []),
        "blocked_sites": data.get("blocked_sites", []),
        "allow_other_sites": data.get("allow_other_sites", True),
        "allow_file_upload": data.get("allow_file_upload", False),
        "disable_widget_on_mobile": bool(data.get("disable_widget_on_mobile", False)),
        "has_files": False,
        "scrape_sites": data.get("scrape_sites", []),
        "scrape_frequency": data.get("scrape_frequency", "manual"),
        "has_scraped_content": False,
        "color": _normalize_color(data.get("color")),
        "text_color": _normalize_text_color(data.get("text_color")),
        "doc_intelligence_enabled": DOC_INTEL_ENABLED and bool(data.get("doc_intelligence_enabled", False)),
        "doc_intelligence_settings": _merge_doc_intel_settings(data.get("doc_intelligence_settings")),
    }

    doc["priority_source"] = _get_priority_source(data)

    if "database" in data:
        doc["database"] = data.get("database")

    result = modes_collection.insert_one(doc)
    doc["_id"] = str(result.inserted_id)
    doc.pop("user_id", None)
    doc.pop("prioritize_files", None)
    doc = _attach_doc_intel_metadata(doc)
    return doc, 201


@routes.put("/admin/modes/<mode_id>")
@cognito_auth_required
def update_mode(mode_id):
    try:
        mode_obj_id = ObjectId(mode_id)
    except Exception:  # noqa: BLE001
        return {"error": "Invalid mode id"}, 400

    doc = modes_collection.find_one({"_id": mode_obj_id})
    if not doc:
        return {"error": "Not found"}, 404
    if not request.user.get("is_super_admin"):
        if not doc.get("user_id") or str(doc.get("user_id")) != str(request.user.get("sub")):
            return {"error": "Access denied"}, 403
    data = request.get_json() or {}
    update = {
        "title": data.get("title", doc.get("title", "")),
        "description": data.get("description", doc.get("description", "")),
        "intro": data.get("intro", doc.get("intro", "")),
        "prompts": data.get("prompts", doc.get("prompts", [])),
        "tags": data.get("tags", doc.get("tags", [])),
        "preferred_sites": data.get("preferred_sites", doc.get("preferred_sites", [])),
        "blocked_sites": data.get("blocked_sites", doc.get("blocked_sites", [])),
        "allow_other_sites": data.get("allow_other_sites", doc.get("allow_other_sites", True)),
        "allow_file_upload": data.get("allow_file_upload", doc.get("allow_file_upload", False)),
        "disable_widget_on_mobile": bool(data.get("disable_widget_on_mobile", doc.get("disable_widget_on_mobile", False))),
        "has_files": doc.get("has_files", False),
        "scrape_sites": data.get("scrape_sites", doc.get("scrape_sites", [])),
        "scrape_frequency": data.get("scrape_frequency", doc.get("scrape_frequency", "manual")),
        "has_scraped_content": doc.get("has_scraped_content", False),
        "color": _normalize_color(data.get("color", doc.get("color", DEFAULT_MODE_COLOR))),
        "text_color": _normalize_text_color(data.get("text_color", doc.get("text_color", DEFAULT_TEXT_COLOR))),
        "doc_intelligence_enabled": DOC_INTEL_ENABLED and bool(data.get("doc_intelligence_enabled", doc.get("doc_intelligence_enabled", False))),
        "doc_intelligence_settings": _merge_doc_intel_settings(data.get("doc_intelligence_settings", doc.get("doc_intelligence_settings"))),
    }

    update["priority_source"] = _get_priority_source(data)

    if "database" in data:
        update["database"] = data.get("database")
    
    modes_collection.update_one({"_id": doc["_id"]}, {"$set": update, "$unset": {"prioritize_files": ""}})
    doc.update(update)
    doc["_id"] = str(doc["_id"])
    doc.pop("user_id", None)
    doc.pop("prioritize_files", None)
    doc = _attach_doc_intel_metadata(doc)
    doc["color"] = _normalize_color(doc.get("color"), DEFAULT_MODE_COLOR)
    doc["text_color"] = _normalize_text_color(doc.get("text_color"), DEFAULT_TEXT_COLOR)
    return doc


@routes.delete("/admin/modes/<mode_id>")
@cognito_auth_required
def delete_mode(mode_id):
    try:
        mode_obj_id = ObjectId(mode_id)
    except Exception:  # noqa: BLE001
        return {"error": "Invalid mode id"}, 400

    doc = modes_collection.find_one({"_id": mode_obj_id})
    if not doc:
        return {"error": "Not found"}, 404
    if not request.user.get("is_super_admin"):
        if not doc.get("user_id") or str(doc.get("user_id")) != str(request.user.get("sub")):
            return {"error": "Access denied"}, 403
    
    mode_name = doc.get("name")
    mode_owner_id = doc.get("user_id")
    
    # Delete all associated documents
    if mode_name:
        doc_query = {"mode": mode_name}
        if not request.user.get("is_super_admin"):
            # Use the stored owner id (string-safe access is handled above).
            doc_query["user_id"] = mode_owner_id
        
        # Find and delete all documents for this mode
        documents = list(documents_collection.find(doc_query))
        for document in documents:
            # Delete from S3
            key = document.get("s3_key")
            if key:
                try:
                    s3.delete_object(Bucket=S3_BUCKET, Key=key)
                except Exception as e:  # noqa: BLE001
                    print("s3 delete failed", e)
            
            # Delete from OpenAI
            if document.get("openai_file_id"):
                try:
                    client.files.delete(document["openai_file_id"])
                except Exception as e:  # noqa: BLE001
                    print("openai file delete failed", e)
                
                # Delete from vector store
                if VECTOR_STORE_ID:
                    try:
                        client.vector_stores.files.delete(
                            vector_store_id=VECTOR_STORE_ID, file_id=document["openai_file_id"]
                        )
                    except Exception as e:  # noqa: BLE001
                        print("vector store delete failed", e)
        
        # Delete all documents from database
        documents_collection.delete_many(doc_query)
    
    # Delete the mode
    modes_collection.delete_one({"_id": doc["_id"]})
    
    return {"success": True, "message": "Mode and all associated documents deleted"}, 200


@routes.get("/admin/login")
def admin_login_page():
    return send_from_directory(routes.static_folder, "admin_login.html")


@routes.post("/admin/login")
def admin_login():
    data = request.get_json() or {}
    username = data.get("username")
    password = data.get("password")
    if not username or not password:
        return {"error": "Username and password required"}, 400
    if not (COGNITO_REGION and COGNITO_APP_CLIENT_ID):
        return {"error": "Cognito not configured"}, 500
    cognito = boto3.client("cognito-idp", region_name=COGNITO_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    try:
        resp = cognito.initiate_auth(
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": username, "PASSWORD": password},
            ClientId=COGNITO_APP_CLIENT_ID,
        )
    except cognito.exceptions.NotAuthorizedException:
        cognito.close()
        return {"error": "Invalid credentials"}, 401
    except Exception as e:  # noqa: BLE001
        print("cognito login failed", e)
        cognito.close()
        return {"error": "Login failed"}, 500
    auth = resp.get("AuthenticationResult", {})
    cognito.close()
    return {
        "id_token": auth.get("IdToken"),
        "access_token": auth.get("AccessToken"),
        "refresh_token": auth.get("RefreshToken"),
    }

@routes.get("/admin/reset")
def reset_password_page():
    # Check if there's a token parameter - if so, show the token-based reset page
    token = request.args.get("token")
    if token:
        return send_from_directory(routes.static_folder, "reset_password.html")
    # Otherwise, show the forgot password page
    return send_from_directory(routes.static_folder, "forgot_password.html")


@routes.post("/admin/reset/initiate")
def admin_forgot_password_initiate():
    """Initiate password reset by generating a token and sending reset link via SES."""
    data = request.get_json() or {}
    username = data.get("username")
    if not username:
        return {"error": "Username is required"}, 400
    if not (COGNITO_REGION and COGNITO_APP_CLIENT_ID and COGNITO_USER_POOL_ID):
        print("Cognito not configured")
        return {"error": "Cognito not configured"}, 500
    if not SES_SENDER_EMAIL:
        print("SES not configured")
        return {"error": "SES not configured"}, 500
    
    cognito = boto3.client("cognito-idp", region_name=COGNITO_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    try:
        # Check if user exists in Cognito and get their email
        try:
            user_response = cognito.admin_get_user(
                UserPoolId=COGNITO_USER_POOL_ID,
                Username=username
            )
            # Extract email from user attributes
            email = None
            for attr in user_response.get('UserAttributes', []):
                if attr['Name'] == 'email':
                    email = attr['Value']
                    break
            
            if not email:
                print(f"No email found for user: {username}")
                user_exists = False
            else:
                user_exists = True
        except cognito.exceptions.UserNotFoundException:
            # User doesn't exist, but we'll still return success (security best practice)
            user_exists = False
            print(f"Password reset requested for non-existent user: {username}")
        
        if user_exists:
            # Generate secure token
            token = secrets.token_urlsafe(32)
            
            # Calculate expiration (15 minutes)
            created_at = datetime.utcnow()
            expires_at = created_at + timedelta(minutes=15)
            
            # Store token in MongoDB
            reset_tokens_collection.insert_one({
                'token': token,
                'email': email,
                'username': username,
                'created_at': created_at,
                'expires_at': expires_at,
                'used': False
            })
            
            # Create indexes if they don't exist
            try:
                reset_tokens_collection.create_index('token', unique=True)
                reset_tokens_collection.create_index('expires_at', expireAfterSeconds=0)
            except Exception:  # noqa: BLE001
                # Indexes might already exist
                print("Failed to create indexes")
                pass
            
            # Generate reset link
            base_url = request.host_url.rstrip('/')
            reset_link = f"{base_url}/flask/admin/reset?token={token}"
            
            # Send email via SES
            try:
                response = _send_password_reset_email(email, reset_link, token_expiry_minutes=15)
                print(f"Password reset email sent successfully to {email}")
                print(f"SES MessageId: {response['MessageId']}")
            except Exception as e:  # noqa: BLE001
                print(f"Failed to send password reset email to {email}: {e}")
                # Still return success to avoid information leakage
        
        # Always return success (don't reveal if user exists or not)
        cognito.close()
        return {"status": "success"}, 200
        
    except Exception as e:  # noqa: BLE001
        print(f"Password reset initiation failed: {e}")
        cognito.close()
        # Still return success to avoid information leakage
        return {"status": "success"}, 200


@routes.post("/admin/reset/token")
def admin_reset_password_with_token():
    """Reset password using a token sent via email (Lambda-generated)."""
    data = request.get_json() or {}
    token = data.get("token")
    new_password = data.get("new_password")
    
    if not token or not new_password:
        return {"error": "Token and new password are required"}, 400
    if not (COGNITO_REGION and COGNITO_APP_CLIENT_ID and COGNITO_USER_POOL_ID):
        return {"error": "Cognito not configured"}, 500
    
    try:
        # Find and validate token in MongoDB
        token_doc = reset_tokens_collection.find_one({"token": token, "used": False})
        
        if not token_doc:
            return {"error": "Invalid or expired reset token"}, 400
        
        # Check if token has expired
        if datetime.utcnow() > token_doc["expires_at"]:
            return {"error": "Reset token has expired. Please request a new one."}, 400
        
        # Get user email from token
        user_email = token_doc.get("email")
        print("user_email", user_email)
        username = token_doc.get("username")
        print("username", username)
        
        if not user_email or not username:
            return {"error": "Invalid token data"}, 400
        
        # Use Cognito admin API to set the new password
        cognito = boto3.client("cognito-idp", region_name=COGNITO_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        
        try:
            # Set password permanently (user is verified)
            cognito.admin_set_user_password(
                UserPoolId=COGNITO_USER_POOL_ID,
                Username=username,
                Password=new_password,
                Permanent=True
            )
            print("cognito admin_set_user_password successful")
        except cognito.exceptions.InvalidPasswordException as e:
            cognito.close()
            error_message = str(e)
            return {"error": f"Invalid password: {error_message}"}, 400
        except cognito.exceptions.UserNotFoundException:
            cognito.close()
            return {"error": "User not found"}, 404
        except Exception as e:  # noqa: BLE001
            cognito.close()
            print(f"Cognito admin_set_user_password failed: {e}")
            return {"error": "Failed to reset password. Please try again."}, 500
        
        cognito.close()
        # Mark token as used
        reset_tokens_collection.update_one(
            {"_id": token_doc["_id"]},
            {"$set": {"used": True, "used_at": datetime.utcnow()}}
        )
        
        print(f"Password reset successful for user: {user_email}")
        return {"status": "success", "message": "Password reset successful"}, 200
        
    except Exception as e:  # noqa: BLE001
        print(f"Token-based password reset failed: {e}")
        return {"error": "Password reset failed. Please try again."}, 500


@routes.post("/api/refresh-token")
def refresh_token():
    """Refresh Cognito tokens using refresh token."""
    data = request.get_json() or {}
    refresh_token = data.get("refresh_token")
    if not refresh_token:
        return {"error": "Refresh token is required"}, 400
    if not (COGNITO_REGION and COGNITO_APP_CLIENT_ID):
        return {"error": "Cognito not configured"}, 500
    
    cognito = boto3.client("cognito-idp", region_name=COGNITO_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    try:
        resp = cognito.initiate_auth(
            AuthFlow="REFRESH_TOKEN_AUTH",
            AuthParameters={"REFRESH_TOKEN": refresh_token},
            ClientId=COGNITO_APP_CLIENT_ID,
        )
    except cognito.exceptions.NotAuthorizedException:
        cognito.close()
        return {"error": "Invalid refresh token"}, 401
    except Exception as e:  # noqa: BLE001
        cognito.close()
        print("cognito refresh failed", e)
        return {"error": "Token refresh failed"}, 500
    
    auth = resp.get("AuthenticationResult", {})
    cognito.close()
    return {
        "id_token": auth.get("IdToken"),
        "access_token": auth.get("AccessToken"),
        "refresh_token": auth.get("RefreshToken", refresh_token),  # Use existing if no new one
    }


@routes.post("/api/permitsca")
@cognito_auth_required
def permitsca_api():
    """API endpoint for permitsca mode that returns JSON responses."""
    data = request.get_json() or {}
    message = (data.get("message") or "").strip()
    tag = (data.get("tag") or "").strip()
    conversation_id = (data.get("conversation_id") or "").strip()
    previous_response_id = (data.get("response_id") or "").strip()
    
    # Force permitsca mode
    mode = "permitsca"
    mode_doc = modes_collection.find_one({"name": mode})
    allow_file_upload = mode_doc.get("allow_file_upload", False) if mode_doc else False
    
    # Handle uploaded file IDs
    uploaded_file_ids = data.get("uploaded_files", [])
    openai_file_ids = uploaded_file_ids if allow_file_upload and uploaded_file_ids else []

    if not message:
        return {"error": "Message is required"}, 400
    
    if not conversation_id:
        conversation_id = str(ObjectId())
    
    ip_addr = request.headers.get("X-Forwarded-For", request.remote_addr)
    print(f"Permitsca API prompt sent from IP: {ip_addr}")
    
    # Log the user prompt
    threading.Thread(
        target=_async_log_prompt,
        kwargs={
            "prompt": message,
            "mode": mode_doc["_id"] if mode_doc else None,
            "ip_addr": ip_addr,
            "conversation_id": conversation_id,
            "prompt_logs_collection": prompt_logs_collection,
        },
    ).start()

    try:
        user_id = request.user.get("sub", "anonymous")
        gpt_text, response_id, usage = conversation_service.respond(
            conversation_id=conversation_id,
            user_id=user_id,
            text=message,
            mode=mode,
            tag=tag,
            previous_response_id=previous_response_id or None,
            file_ids=openai_file_ids,
        )

        # Log the AI response
        threading.Thread(
            target=_async_log_prompt,
            kwargs={
                "response": gpt_text,
                "mode": mode_doc["_id"] if mode_doc else None,
                "ip_addr": ip_addr,
                "conversation_id": conversation_id,
                "response_id": response_id,
                "prompt_logs_collection": prompt_logs_collection,
            },
        ).start()

        # Store uploaded files with the conversation for future use
        if openai_file_ids:
            conversation_service.store_conversation_files(conversation_id, openai_file_ids)

        return {
            "response": gpt_text,
            "conversation_id": conversation_id,
            "response_id": response_id,
            "usage": usage,
            "mode": mode,
            "tag": tag,
        }
    except Exception as err:  # noqa: BLE001
        print("Error getting AI response:", err)
        return {"error": "There was an error getting a response. Please try again."}, 500


@routes.post("/api/scrape-target")
@token_auth_required
def scrape_target_api():
    """
    Universal target scrape endpoint.

    Body:
      {
        "url": "https://...",
        "options": { ... },             // optional; merged into query params
        "target": {"type":"div","selectors":{...}},
        "execution": "sync"|"async",    // default async
        "timeout_ms": 30000             // optional
      }
    """
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    if not url:
        return {"error": "url is required"}, 400

    options = data.get("options", None)
    if options is not None and not isinstance(options, dict):
        return {"error": "options must be an object"}, 400

    target = data.get("target")
    if not isinstance(target, dict):
        return {"error": "target is required and must be an object"}, 400
    target_type = (target.get("type") or "").strip()
    selectors = target.get("selectors")
    if not target_type:
        return {"error": "target.type is required"}, 400
    if selectors is None or not isinstance(selectors, dict):
        return {"error": "target.selectors is required and must be an object"}, 400

    execution = (data.get("execution") or "async").strip().lower()
    if execution not in ("async", "sync"):
        return {"error": "execution must be 'sync' or 'async'"}, 400

    timeout_ms = data.get("timeout_ms", 30000)
    try:
        timeout_ms = int(timeout_ms)
    except Exception:  # noqa: BLE001
        return {"error": "timeout_ms must be an integer"}, 400
    if timeout_ms <= 0 or timeout_ms > 300000:
        return {"error": "timeout_ms must be between 1 and 300000"}, 400

    token_user_id = (getattr(request, "api_token", {}) or {}).get("user_id")
    user_id = str(token_user_id) if token_user_id is not None else "api"

    # Async path: create job + dispatch to the configured backend (local thread or SQS worker)
    if execution == "async":
        try:
            job_id = scraper_client.queue_api_target_scrape(
                url=url,
                options=options or None,
                target={"type": target_type, "selectors": selectors},
                user_id=user_id,
                timeout_ms=timeout_ms,
                auto_dispatch=True,
            )
            return {"status": "queued", "job_id": str(job_id)}, 202
        except Exception as e:  # noqa: BLE001
            print(f"Error queueing api_target_scrape job: {e}")
            return {"error": "Failed to queue job", "details": str(e)}, 500

    # Sync path: run immediately and (optionally) persist a job doc for inspectability
    global scraping_service  # reuse existing instance if present
    if scraping_service is None:
        try:
            scraping_service = ScrapingService(
                client,
                db,
                VECTOR_STORE_ID,
            )
        except Exception as e:  # noqa: BLE001
            print(f"Error initializing scraping_service for sync scrape-target: {e}")
            return {"error": "Scraping service not available", "details": str(e)}, 500

    job_doc = {
        "job_type": "api_target_scrape",
        "status": "in_progress",
        "user_id": user_id,
        "url": url,
        "options": options or None,
        "target": {"type": target_type, "selectors": selectors},
        "timeout_ms": timeout_ms,
        "result": None,
        "error": None,
        "created_at": datetime.utcnow(),
        "started_at": datetime.utcnow(),
        "completed_at": None,
        "environment": SCRAPER_ENVIRONMENT if SCRAPER_EXECUTION_MODE != "local" else ("dev" if localDevMode == "true" else "prod"),
    }
    job_id = scraping_jobs_collection.insert_one(job_doc).inserted_id

    try:
        matches = scraping_service.scrape_target_elements(
            url,
            options=options or None,
            target={"type": target_type, "selectors": selectors},
            timeout_ms=timeout_ms,
        )
        if not matches:
            scraping_jobs_collection.update_one(
                {"_id": job_id},
                {"$set": {"status": "failed", "error": "No matching elements found", "completed_at": datetime.utcnow()}},
            )
            return {"error": "No matching elements found", "job_id": str(job_id)}, 404

        result = {"match_count": len(matches), "matches": matches}
        scraping_jobs_collection.update_one(
            {"_id": job_id},
            {"$set": {"status": "completed", "result": result, "completed_at": datetime.utcnow()}},
        )
        return {"status": "completed", "job_id": str(job_id), "result": result}, 200
    except PlaywrightTimeoutError:
        scraping_jobs_collection.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "status": "failed",
                    "error": f"Timeout waiting for target selector (timeout_ms={timeout_ms})",
                    "completed_at": datetime.utcnow(),
                }
            },
        )
        return {"error": "Timeout waiting for target selector", "job_id": str(job_id)}, 504
    except Exception as e:  # noqa: BLE001
        scraping_jobs_collection.update_one(
            {"_id": job_id},
            {"$set": {"status": "failed", "error": str(e), "completed_at": datetime.utcnow()}},
        )
        print(f"Error running sync scrape-target: {e}")
        return {"error": "Failed to scrape target", "details": str(e), "job_id": str(job_id)}, 500


@routes.get("/admin")
def admin_page():
    return send_from_directory(routes.static_folder, "admin.html")


@routes.get("/admin/analytics")
def admin_analytics_page():
    return send_from_directory(routes.static_folder, "admin_analytics.html")


@routes.get("/admin/mode")
def admin_mode_page():
    return send_from_directory(routes.static_folder, "mode_editor.html")


@routes.get("/admin/how-to")
def admin_how_to_page():
    return send_from_directory(routes.static_folder, "how-to.html")


@routes.get("/admin/analytics/summary")
@cognito_auth_required
def admin_analytics_summary():
    start_param = (request.args.get("start_date") or "").strip()
    end_param = (request.args.get("end_date") or "").strip()
    mode = (request.args.get("mode") or "").strip()
    search = (request.args.get("search") or "").strip()

    match = {}
    date_filter = {}
    start_dt = _parse_date(start_param)
    end_dt = _parse_date(end_param, end=True)
    if start_dt:
        date_filter["$gte"] = start_dt
    if end_dt:
        date_filter["$lte"] = end_dt
    if date_filter:
        match["created_at"] = date_filter
    if mode:
        match["mode"] = mode
    if search:
        match["prompt"] = {"$regex": re.escape(search), "$options": "i"}

    # Create a separate match filter for user prompts only (excludes AI responses)
    prompt_match = {**match, "prompt": {"$exists": True}}
    if search:
        # If search is specified, it already includes the prompt filter with regex
        prompt_match["prompt"] = {"$regex": re.escape(search), "$options": "i"}
    
    pipeline = [{"$match": prompt_match}]

    def _isoformat_with_z(dt):
        if not dt:
            return None
        iso_value = dt.isoformat()
        if iso_value.endswith("Z") or "+" in iso_value[10:]:
            return iso_value
        return f"{iso_value}Z"

    total_prompts = prompt_logs_collection.count_documents(prompt_match)
    total_responses = prompt_logs_collection.count_documents({**match, "response": {"$exists": True}})
    conversation_ids = [
        cid for cid in prompt_logs_collection.distinct("conversation_id", prompt_match) if cid
    ]
    unique_conversations = len(conversation_ids)
    ip_hashes = [
        ip for ip in prompt_logs_collection.distinct("ip_hash", prompt_match) if ip
    ]
    unique_users = len(ip_hashes)

    # Get mode IDs and their counts
    mode_counts = [
        {"mode_id": doc.get("_id"), "count": doc.get("count", 0)}
        for doc in prompt_logs_collection.aggregate(
            pipeline
            + [
                {"$group": {"_id": "$mode", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10},
            ]
        )
    ]
    
    # Convert mode IDs to mode titles
    top_modes = []
    for mode_data in mode_counts:
        mode_id = mode_data["mode_id"]
        if mode_id:
            try:
                mode_doc = modes_collection.find_one({"_id": ObjectId(mode_id)})
                mode_title = mode_doc.get("title") or mode_doc.get("name") if mode_doc else "Unknown"
            except Exception as e:
                print(f"Error converting mode ID to title: {e}")
                mode_title = "Unknown"
        else:
            mode_title = "Unknown"
            print("Mode ID is None")
        
        top_modes.append({
            "mode": mode_title,
            "count": mode_data["count"]
        })

    daily_counts = [
        {"date": doc.get("_id"), "count": doc.get("count", 0)}
        for doc in prompt_logs_collection.aggregate(
            pipeline
            + [
                {
                    "$group": {
                        "_id": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$created_at",
                            }
                        },
                        "count": {"$sum": 1},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
        )
    ]

    top_locations = [
        {"country": doc.get("_id") or "Unknown", "count": doc.get("count", 0)}
        for doc in prompt_logs_collection.aggregate(
            pipeline
            + [
                {
                    "$group": {
                        "_id": {"$ifNull": ["$location.country", "Unknown"]},
                        "count": {"$sum": 1},
                    }
                },
                {"$sort": {"count": -1}},
                {"$limit": 10},
            ]
        )
    ]

    top_cities = [
        {
            "city": doc.get("_id", {}).get("city") or "Unknown",
            "country": doc.get("_id", {}).get("country") or "Unknown",
            "count": doc.get("count", 0)
        }
        for doc in prompt_logs_collection.aggregate(
            pipeline
            + [
                {
                    "$group": {
                        "_id": {
                            "city": {"$ifNull": ["$location.city", "Unknown"]},
                            "country": {"$ifNull": ["$location.country", "Unknown"]}
                        },
                        "count": {"$sum": 1},
                    }
                },
                {"$sort": {"count": -1}},
                {"$limit": 15},
            ]
        )
    ]

    hourly_counts = [
        {"hour": doc.get("_id"), "count": doc.get("count", 0)}
        for doc in prompt_logs_collection.aggregate(
            pipeline
            + [
                {"$group": {"_id": {"$hour": "$created_at"}, "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}},
            ]
        )
    ]

    # Get recent conversations (grouped by conversation_id)
    recent_conversations = []
    conversation_aggregation = prompt_logs_collection.aggregate([
        {"$match": prompt_match},
        {"$group": {
            "_id": "$conversation_id",
            "last_updated": {"$max": "$created_at"},
            "first_message": {"$min": "$created_at"},
            "message_count": {"$sum": 1},  # Already filtered to prompts only
            "modes": {"$addToSet": "$mode"},
            "first_prompt": {"$first": "$prompt"}
        }},
        {"$sort": {"last_updated": -1}},
        {"$limit": 20}
    ])
    
    for conv in conversation_aggregation:
        conversation_id = conv.get("_id")
        if not conversation_id:
            continue
        
        last_updated = conv.get("last_updated")
        first_message = conv.get("first_message")
        message_count = conv.get("message_count", 0)
        mode_ids = conv.get("modes", [])
        first_prompt = conv.get("first_prompt", "")
        
        # Convert mode IDs to mode titles
        mode_titles = []
        for mode_id in mode_ids:
            if mode_id:
                try:
                    mode_doc = modes_collection.find_one({"_id": ObjectId(mode_id)})
                    mode_title = mode_doc.get("title") or mode_doc.get("name") if mode_doc else "Unknown"
                    mode_titles.append(mode_title)
                except Exception as e:
                    print(f"Error converting mode ID to title: {e}")
        
        # Get a preview of the first prompt (truncate if too long)
        preview = first_prompt[:150] + "..." if len(first_prompt) > 150 else first_prompt
        
        recent_conversations.append({
            "conversation_id": conversation_id,
            "last_updated": _isoformat_with_z(last_updated),
            "first_message": _isoformat_with_z(first_message),
            "message_count": message_count,
            "modes": mode_titles,
            "preview": preview
        })

    # Convert available mode IDs to mode titles and return both
    available_mode_ids = [m for m in prompt_logs_collection.distinct("mode") if m]
    available_modes = []
    for mode_id in available_mode_ids:
        try:
            mode_doc = modes_collection.find_one({"_id": ObjectId(mode_id)})
            mode_title = mode_doc.get("title") or mode_doc.get("name") if mode_doc else "Unknown"
            available_modes.append({
                "id": mode_id,
                "title": mode_title
            })
        except Exception as e:
            print(f"Error converting mode ID to title: {e}")
            available_modes.append({
                "id": mode_id,
                "title": "Unknown"
            })
    available_modes = sorted(available_modes, key=lambda x: x["title"])

    first_log = prompt_logs_collection.find_one({}, sort=[("created_at", 1)])
    last_log = prompt_logs_collection.find_one({}, sort=[("created_at", -1)])

    start_iso = _isoformat_with_z(first_log.get("created_at")) if first_log else None
    end_iso = _isoformat_with_z(last_log.get("created_at")) if last_log else None
    global_date_range = (
        {"start": start_iso, "end": end_iso}
        if start_iso and end_iso
        else None
    )

    return {
        "total_prompts": total_prompts,
        "total_responses": total_responses,
        "unique_conversations": unique_conversations,
        "unique_users": unique_users,
        "top_modes": top_modes,
        "daily_counts": daily_counts,
        "top_locations": top_locations,
        "top_cities": top_cities,
        "hourly_counts": hourly_counts,
        "recent_conversations": recent_conversations,
        "available_modes": available_modes,
        "global_date_range": global_date_range,
    }


@routes.get("/admin/analytics/search")
@cognito_auth_required
def admin_analytics_search():
    query = (request.args.get("query") or "").strip()
    if not query:
        return {"error": "Query is required"}, 400
    
    start_param = (request.args.get("start_date") or "").strip()
    end_param = (request.args.get("end_date") or "").strip()
    mode = (request.args.get("mode") or "").strip()
    search = (request.args.get("search") or "").strip()

    # Build the same match criteria as the main analytics endpoint
    match = {}
    date_filter = {}
    start_dt = _parse_date(start_param)
    end_dt = _parse_date(end_param, end=True)
    if start_dt:
        date_filter["$gte"] = start_dt
    if end_dt:
        date_filter["$lte"] = end_dt
    if date_filter:
        match["created_at"] = date_filter
    if mode:
        match["mode"] = mode
    if search:
        match["prompt"] = {"$regex": re.escape(search), "$options": "i"}

    # Create a filter for user prompts only (excludes AI responses)
    prompt_match = {**match, "prompt": {"$exists": True}}
    if search:
        # If search is specified, it already includes the prompt filter with regex
        prompt_match["prompt"] = {"$regex": re.escape(search), "$options": "i"}
    
    pipeline = [{"$match": prompt_match}]

    try:
        # Use AI to interpret the query and generate appropriate analytics
        result = _process_natural_language_query(query, pipeline, prompt_match, client, prompt_logs_collection, modes_collection)
        return result
    except Exception as e:
        print(f"Error processing natural language query: {e}")
        return {"error": "Unable to process your question. Please try rephrasing it."}, 500


@routes.get("/admin/analytics/conversations/<conversation_id>/prompts")
@cognito_auth_required
def admin_analytics_conversation_prompts(conversation_id):
    """Get all prompt logs for a specific conversation, ordered oldest first."""
    try:
        # Find all prompt_logs matching this conversation_id
        prompts = []
        for doc in prompt_logs_collection.find({"conversation_id": conversation_id}).sort("created_at", 1):
            created_at = doc.get("created_at")
            mode_id = doc.get("mode")
            
            # Convert mode ID to mode title
            mode_title = "Unknown"
            if mode_id:
                try:
                    mode_doc = modes_collection.find_one({"_id": ObjectId(mode_id)})
                    mode_title = mode_doc.get("title") or mode_doc.get("name") if mode_doc else "Unknown"
                except Exception as e:
                    mode_title = "Unknown"
                    print(f"Error converting mode ID to title: {e}")
            
            prompts.append({
                "prompt": doc.get("prompt", ""),
                "response": doc.get("response", ""),
                "mode": mode_title,
                "created_at": created_at.isoformat() + "Z" if created_at else None,
            })
        
        return {"prompts": prompts}
    except Exception as e:
        print(f"Error fetching conversation prompts: {e}")
        return {"error": "Failed to fetch conversation prompts"}, 500


@routes.get("/admin/user")
@cognito_auth_required
def get_user_info():
    return {
        "user_id": request.user["sub"],
        "is_super_admin": request.user.get("is_super_admin", False)
    }


@routes.get("/admin/superadmin/overview")
@cognito_auth_required
def superadmin_overview():
    """Get overview of all admin users and their modes - superadmin only."""
    if not request.user.get("is_super_admin"):
        return {"error": "Unauthorized - superadmin only"}, 403
    
    try:
        # Get all modes with user_id field
        all_modes = list(modes_collection.find({"user_id": {"$exists": True}}))
        
        # Group modes by user_id
        user_modes_map = {}
        for mode in all_modes:
            user_id = mode.get("user_id")
            if user_id:
                if user_id not in user_modes_map:
                    user_modes_map[user_id] = []
                user_modes_map[user_id].append({
                    "_id": str(mode["_id"]),
                    "name": mode.get("name", "Untitled"),
                    "title": mode.get("title", mode.get("name", "Untitled")),
                    "description": mode.get("description", "")
                })
        
        # Resolve user_ids to usernames using Cognito
        cognito = boto3.client(
            "cognito-idp",
            region_name=COGNITO_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        
        admin_users = []
        for user_id, modes in user_modes_map.items():
            username = None
            email = None
            
            try:
                # Get user details from Cognito using list_users with filter
                user_response = cognito.list_users(
                    UserPoolId=COGNITO_USER_POOL_ID,
                    Filter=f'sub = "{user_id}"'
                )
                
                # list_users returns a list, get the first user if found
                users = user_response.get("Users", [])
                if users:
                    user_data = users[0]
                    
                    # Extract username (Username field)
                    username = user_data.get("Username", user_id)
                    
                    # Extract email from attributes
                    for attr in user_data.get("Attributes", []):
                        if attr["Name"] == "email":
                            email = attr["Value"]
                            break
                else:
                    print(f"User not found in Cognito: {user_id}")
                    username = f"Unknown ({user_id[:8]}...)"
                        
            except Exception as e:
                print(f"Error fetching user from Cognito: {e}")
                username = f"Error ({user_id[:8]}...)"
            
            admin_users.append({
                "user_id": user_id,
                "username": username or user_id,
                "email": email,
                "mode_count": len(modes),
                "modes": sorted(modes, key=lambda x: x["name"].lower())
            })
        
        cognito.close()
        
        # Sort by mode count (descending) then username
        admin_users.sort(key=lambda x: (-x["mode_count"], x["username"].lower()))
        
        return {
            "total_admins": len(admin_users),
            "total_modes": sum(user["mode_count"] for user in admin_users),
            "admin_users": admin_users
        }
        
    except Exception as e:
        print(f"Error in superadmin_overview: {e}")
        return {"error": "Failed to fetch admin overview"}, 500


@routes.get("/admin/superadmin")
def superadmin_overview_page():
    return send_from_directory(routes.static_folder, "admin_superadmin_overview.html")


@routes.get("/admin/documents")
@cognito_auth_required
def list_documents_admin():
    mode = (request.args.get("mode") or "").strip()
    docs = []
    if request.user.get("is_super_admin"):
        query = {}
        if mode:
            query["mode"] = mode
        cursor = documents_collection.find(query)
    else:
        pipeline = [{"$match": {"$expr": {"$eq": [{"$toString": "$user_id"}, str(request.user.get("sub"))]}}}]
        if mode:
            pipeline.insert(0, {"$match": {"mode": mode}})
        cursor = documents_collection.aggregate(pipeline)
    for d in cursor:
        d["_id"] = str(d["_id"])
        docs.append(d)
    return {"documents": docs}


@routes.post("/admin/documents")
@cognito_auth_required
def create_document():
    mode = (request.form.get("mode") or "").strip()
    content = request.form.get("content") or ""
    tag = (request.form.get("tag") or "").strip()
    always_include = request.form.get("always_include") == "true"
    file = request.files.get("file")
    s3_key = None
    openai_file_id = None

    mode_doc = None
    if mode:
        if request.user.get("is_super_admin"):
            mode_doc = modes_collection.find_one({"name": mode})
        else:
            mode_doc = next(
                modes_collection.aggregate([
                    {"$match": {"name": mode}},
                    {"$match": {"$expr": {"$eq": [{"$toString": "$user_id"}, str(request.user.get("sub"))]}}},
                    {"$limit": 1},
                ]),
                None,
            )
    mode_owner_id = (mode_doc or {}).get("user_id", request.user["sub"])
    allowed_tags = mode_doc.get("tags", []) if mode_doc else []
    if tag and tag not in allowed_tags:
        return {"error": "Invalid tag"}, 400

    if file:
        data = file.read()
        filename = file.filename
        key = f"{mode}/{tag or 'untagged'}/{filename}"
        meta = {"always-include": "true"} if always_include else {}
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=data,
            ContentType=file.content_type,
            Metadata=meta,
        )
        s3_key = key
        file_stream = io.BytesIO(data)
        openai_file = client.files.create(file=(filename, file_stream), purpose="assistants")
        openai_file_id = openai_file.id
        if VECTOR_STORE_ID:
            try:
                vs_meta = {"mode": mode, "tag": tag}
                if always_include:
                    vs_meta["always_include"] = "true"
                client.vector_stores.files.create(
                    vector_store_id=VECTOR_STORE_ID,
                    file_id=openai_file_id,
                    attributes=vs_meta,
                )
            except Exception as e:  # noqa: BLE001
                print("vector store add failed", e)
    doc = {
        "user_id": mode_owner_id,
        "mode": mode,
        "content": content,
        "tag": tag,
        "s3_key": s3_key,
        "openai_file_id": openai_file_id,
        "always_include": always_include,
    }
    result = documents_collection.insert_one(doc)
    doc["_id"] = str(result.inserted_id)
    
    # If a file was uploaded, update the mode's has_files field to True
    if file and mode_doc:
        modes_collection.update_one(
            {"_id": mode_doc["_id"]}, 
            {"$set": {"has_files": True}}
        )
    
    return doc, 201


@routes.delete("/admin/documents/<doc_id>")
@cognito_auth_required
def delete_document(doc_id):
    try:
        doc_obj_id = ObjectId(doc_id)
    except Exception:  # noqa: BLE001
        return {"error": "Invalid document id"}, 400

    doc = documents_collection.find_one({"_id": doc_obj_id})
    if not doc:
        return {"error": "Not found"}, 404
    if not request.user.get("is_super_admin"):
        if not doc.get("user_id") or str(doc.get("user_id")) != str(request.user.get("sub")):
            return {"error": "Access denied"}, 403
    key = doc.get("s3_key")
    if key:
        try:
            s3.delete_object(Bucket=S3_BUCKET, Key=key)
        except Exception as e:  # noqa: BLE001
            print("s3 delete failed", e)
    if doc.get("openai_file_id"):
        try:
            client.files.delete(doc["openai_file_id"])
        except Exception as e:  # noqa: BLE001
            print("openai file delete failed", e)
        if VECTOR_STORE_ID:
            try:
                client.vector_stores.files.delete(
                    vector_store_id=VECTOR_STORE_ID, file_id=doc["openai_file_id"]
                )
            except Exception as e:  # noqa: BLE001
                print("vector store delete failed", e)
    documents_collection.delete_one({"_id": doc["_id"]})
    
    # Check if there are any remaining files for this mode
    if doc.get("mode"):
        # Find the mode owned by the same owner as the document (mode names can repeat across users).
        mode_doc = None
        for candidate in modes_collection.find({"name": doc["mode"]}, {"_id": 1, "user_id": 1}):
            if str(candidate.get("user_id")) == str(doc.get("user_id")):
                mode_doc = candidate
                break
        
        if mode_doc:
            # Count remaining documents with files for this mode
            remaining_files_count = documents_collection.count_documents({
                "mode": doc["mode"],
                "s3_key": {"$exists": True, "$ne": None}
            })
            
            # If no files remain, set has_files to False
            if remaining_files_count == 0:
                modes_collection.update_one(
                    {"_id": mode_doc["_id"]}, 
                    {"$set": {"has_files": False}}
                )
    
    return {"status": "deleted"}


@routes.get("/admin/documents/<doc_id>/download")
@cognito_auth_required
def download_document(doc_id):
    try:
        doc_obj_id = ObjectId(doc_id)
    except Exception:  # noqa: BLE001
        return {"error": "Invalid document id"}, 400

    doc = documents_collection.find_one({"_id": doc_obj_id})
    if not doc:
        return {"error": "Not found"}, 404
    if not request.user.get("is_super_admin"):
        if not doc.get("user_id") or str(doc.get("user_id")) != str(request.user.get("sub")):
            return {"error": "Access denied"}, 403
    
    s3_key = doc.get("s3_key")
    if not s3_key:
        return {"error": "No file associated with this document"}, 404
    
    try:
        # Get the file from S3
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        
        # Extract filename from s3_key (format: mode/tag/filename)
        filename = s3_key.split('/')[-1]
        
        # Return the file as a downloadable response
        return Response(
            response['Body'].read(),
            mimetype=response['ContentType'],
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            }
        )
    except Exception as e:
        print("s3 download failed", e)
        return {"error": "Failed to download file"}, 500


def _doc_intel_guard():
    if not doc_intelligence_service:
        return {"error": "Document intelligence is disabled"}, 400
    return None


@routes.get("/doc-intel/summary")
def doc_intel_summary():
    guard = _doc_intel_guard()
    if guard:
        return guard
    mode_name = (request.args.get("mode") or "").strip()
    session_id = (request.args.get("session_id") or "").strip()
    if not session_id:
        return {"error": "session_id is required"}, 400
    mode_doc, error = _doc_intel_mode_lookup(mode_name)
    if error:
        return error
    summary = doc_intelligence_service.get_project_summary(session_id)
    if not summary:
        return {"project_id": None, "file_count": 0, "package_count": 0, "files": [], "packages": []}
    return summary


@routes.post("/doc-intel/ingest")
def doc_intel_ingest():
    guard = _doc_intel_guard()
    if guard:
        return guard
    mode_name = (request.form.get("mode") or "").strip()
    session_id = (request.form.get("session_id") or "").strip()
    
    logger.info(f"DocIntel ingest requested: mode={mode_name}, session={session_id}")
    
    if not session_id:
        return {"error": "session_id is required"}, 400
    mode_doc, error = _doc_intel_mode_lookup(mode_name)
    if error:
        return error
    files = request.files.getlist("files")
    if not files:
        return {"error": "At least one file is required"}, 400
    
    logger.info(f"Validating {len(files)} files for ingestion")
    validation_error = _doc_intel_validate_files(files)
    if validation_error:
        logger.warning(f"Validation failed: {validation_error}")
        return validation_error
    try:
        result = doc_intelligence_service.ingest_files(mode_doc, session_id, files)
        logger.info("Ingestion completed successfully")
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error(f"Ingestion error: {exc}", exc_info=True)
        return {"error": str(exc)}, 500


@routes.get("/doc-intel/search")
def doc_intel_search():
    guard = _doc_intel_guard()
    if guard:
        return guard
    mode_name = (request.args.get("mode") or "").strip()
    session_id = (request.args.get("session_id") or "").strip()
    if not session_id:
        return {"error": "session_id is required"}, 400
    mode_doc, error = _doc_intel_mode_lookup(mode_name)
    if error:
        return error
    query_text = (request.args.get("query") or "").strip()
    if not query_text:
        return {"error": "query is required"}, 400
    filters = {}
    trade = (request.args.get("trade") or "").strip().lower()
    if trade:
        filters["trade"] = trade
    results = doc_intelligence_service.search(session_id, query_text, filters or None)
    return {"results": results}


@routes.post("/doc-intel/query-search")
def doc_intel_query_search():
    """Dedicated endpoint for search mode that bypasses the conversation service."""
    guard = _doc_intel_guard()
    if guard:
        return guard
        
    data = request.get_json(silent=True) or request.form.to_dict() or {}
    mode_name = (data.get("mode") or "").strip()
    session_id = (data.get("session_id") or "").strip()
    query_text = (data.get("query") or "").strip()
    
    if not session_id:
        return {"error": "session_id is required"}, 400
    if not query_text:
        return {"error": "query is required"}, 400
        
    mode_doc, error = _doc_intel_mode_lookup(mode_name)
    if error:
        return error
        
    # Check for filters
    filters = data.get("filters", {})
    
    try:
        # Perform the search using doc intelligence service
        results = doc_intelligence_service.search(session_id, query_text, filters or None)
        formatted_results = doc_intelligence_service._format_search_results(results, {"action": "search", "query": query_text})
        
        # Convert to HTML response format matching the chat interface
        html_reply = markdown(formatted_results)
        
        # Auto-link plain URLs
        def _linkify(match):
            url = match.group(0)
            return f'<a href="{url}" target="_blank" rel="noopener">{url}</a>'

        html_reply = re.sub(r'(?<!href=")(https?://[^\s<]+)', _linkify, html_reply)
        
        html_reply = bleach.clean(
            html_reply,
            tags=list(bleach.sanitizer.ALLOWED_TAGS) + ["img", "p", "h3", "br", "ul", "li", "strong", "em"],
            attributes={"a": ["href", "target", "rel"], "img": ["src", "alt"]},
        )
        
        response_id = str(ObjectId())
        conversation_id = data.get("conversation_id") or str(ObjectId())
        
        html = (
            '<div class="chat-entry assistant">'
            '<div class="bubble markdown">'
            f"{html_reply}"
            "</div></div>"
            f'<input type="hidden" id="conversation_id" name="conversation_id" value="{conversation_id}" hx-swap-oob="true"/>'
            f'<input type="hidden" id="response_id" name="response_id" value="{response_id}" hx-swap-oob="true"/>'
        )
        
        return html
        
    except Exception as exc:  # noqa: BLE001
        print(f"Error in doc-intel-query-search: {exc}")
        return (
            '<div class="chat-entry assistant">'
            '<div class="bubble">❌ There was an error searching your documents. Please try again.</div>'
            "</div>"
        ), 500


@routes.post("/doc-intel/build-package")
def doc_intel_build_package():
    guard = _doc_intel_guard()
    if guard:
        return guard
    data = request.get_json() or {}
    mode_name = (data.get("mode") or "").strip()
    session_id = (data.get("session_id") or "").strip()
    if not session_id:
        return {"error": "session_id is required"}, 400
    mode_doc, error = _doc_intel_mode_lookup(mode_name)
    if error:
        return error
    plan = data.get("plan") or {}
    output = (data.get("output") or "pdf").lower()
    if output not in {"pdf", "zip"}:
        return {"error": "output must be 'pdf' or 'zip'"}, 400
    if not plan:
        return {"error": "plan is required"}, 400
    try:
        package = doc_intelligence_service.build_package(mode_doc, session_id, plan, output)
        package["download_url"] = url_for(
            "routes.doc_intel_package_download",
            package_id=package["package_id"],
            mode=mode_name,
            session_id=session_id,
            file_type="zip" if package.get("output_zip_path") else "pdf",
            _external=False,
        )
        return {"package": package}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}, 500


@routes.get("/doc-intel/package/<package_id>")
def doc_intel_package_download(package_id):
    guard = _doc_intel_guard()
    if guard:
        return guard
    mode_name = (request.args.get("mode") or "").strip()
    file_type = (request.args.get("file_type") or "pdf").lower()
    session_id = (request.args.get("session_id") or "").strip()
    if not session_id:
        return {"error": "session_id is required"}, 400
    mode_doc, error = _doc_intel_mode_lookup(mode_name)
    if error:
        return error
    package = doc_intelligence_service.get_package(session_id, package_id)
    if not package:
        return {"error": "Package not found"}, 404
    file_path = package.output_zip_path if file_type == "zip" else package.output_pdf_path
    if not file_path or not os.path.exists(file_path):
        return {"error": "Package file unavailable"}, 404
    return send_file(file_path, as_attachment=True, download_name=os.path.basename(file_path))


@routes.get("/doc-intel/extract")
def doc_intel_extract():
    guard = _doc_intel_guard()
    if guard:
        return guard
    mode_name = (request.args.get("mode") or "").strip()
    session_id = (request.args.get("session_id") or "").strip()
    query_text = (request.args.get("query") or "").strip()
    if not query_text:
        return {"error": "query is required"}, 400
    if not session_id:
        return {"error": "session_id is required"}, 400
    mode_doc, error = _doc_intel_mode_lookup(mode_name)
    if error:
        return error
    filters = {}
    trade = (request.args.get("trade") or "").strip().lower()
    if trade:
        filters["trade"] = trade
    payload = doc_intelligence_service.structured_extract_payload(mode_doc, session_id, query_text, filters or None)
    return payload


@routes.post("/doc-intel/auto-package")
def doc_intel_auto_package():
    guard = _doc_intel_guard()
    if guard:
        return guard
    data = request.get_json() or {}
    mode_name = (data.get("mode") or "").strip()
    session_id = (data.get("session_id") or "").strip()
    mode_doc, error = _doc_intel_mode_lookup(mode_name)
    if error:
        return error
    if not session_id:
        return {"error": "session_id is required"}, 400
    trade = (data.get("trade") or "").strip().lower() or None
    output = (data.get("output") or "pdf").lower()
    instructions = data.get("instructions")
    try:
        result = doc_intelligence_service.build_package_from_intent(
            mode_doc,
            session_id,
            trade=trade,
            output=output,
            filters=data.get("filters"),
            query=instructions,
        )
        package = result["package"]
        file_type = "zip" if package.get("output_zip_path") else "pdf"
        package["download_url"] = (
            f"/flask/doc-intel/package/{package['package_id']}?mode={mode_name}"
            f"&session_id={session_id}&file_type={file_type}"
        )
        return result
    except Exception as exc:  # noqa: BLE001
        app.logger.error(f"Error building package: {exc}", exc_info=True)
        return {"error": str(exc)}, 500


@routes.post("/doc-intel/propose-package")
def doc_intel_propose_package():
    guard = _doc_intel_guard()
    if guard:
        return guard
    data = request.get_json() or {}
    mode_name = (data.get("mode") or "").strip()
    session_id = (data.get("session_id") or "").strip()
    
    mode_doc, error = _doc_intel_mode_lookup(mode_name)
    if error:
        return error
    if not session_id:
        return {"error": "session_id is required"}, 400
        
    trade = (data.get("trade") or "").strip().lower() or None
    instructions = data.get("instructions")
    filters = data.get("filters")
    
    try:
        proposal = doc_intelligence_service.propose_bid_package(
            mode_doc,
            session_id,
            trade=trade,
            filters=filters,
            query=instructions
        )
        return proposal
    except Exception as exc:  # noqa: BLE001
        app.logger.error(f"Error proposing package: {exc}", exc_info=True)
        return {"error": str(exc)}, 500


@routes.post("/doc-intel/build-package-selection")
def doc_intel_build_package_selection():
    guard = _doc_intel_guard()
    if guard:
        return guard
    data = request.get_json() or {}
    mode_name = (data.get("mode") or "").strip()
    session_id = (data.get("session_id") or "").strip()
    
    mode_doc, error = _doc_intel_mode_lookup(mode_name)
    if error:
        return error
    if not session_id:
        return {"error": "session_id is required"}, 400
        
    file_ids = data.get("file_ids") or []
    if not file_ids:
        return {"error": "No files selected"}, 400
        
    plan_details = data.get("plan_details") or {}
    output = (data.get("output") or "pdf").lower()
    
    try:
        package = doc_intelligence_service.build_package_from_selection(
            mode_doc,
            session_id,
            file_ids,
            plan_details,
            output
        )
        file_type = "zip" if package.get("output_zip_path") else "pdf"
        package["download_url"] = (
            f"/flask/doc-intel/package/{package['package_id']}?mode={mode_name}"
            f"&session_id={session_id}&file_type={file_type}"
        )
        return {"package": package}
    except Exception as exc:  # noqa: BLE001
        app.logger.error(f"Error building package from selection: {exc}", exc_info=True)
        return {"error": str(exc)}, 500


@routes.post("/admin/scrape/trigger/<mode_id>")
@cognito_auth_required
def trigger_scrape(mode_id):
    """Manually trigger scraping for a specific mode (runs in background)."""
    try:
        mode_obj_id = ObjectId(mode_id)
    except Exception:  # noqa: BLE001
        return {"error": "Invalid mode id"}, 400

    mode_doc = modes_collection.find_one({"_id": mode_obj_id})
    if not mode_doc:
        return {"error": "Mode not found"}, 404
    if not request.user.get("is_super_admin"):
        if not mode_doc.get("user_id") or str(mode_doc.get("user_id")) != str(request.user.get("sub")):
            return {"error": "Access denied"}, 403
    
    mode_name = mode_doc.get("name")
    scrape_sites = mode_doc.get("scrape_sites", [])
    
    if not scrape_sites:
        return {"error": "No sites configured for scraping"}, 400
    
    try:
        # Trigger background scrape (non-blocking)
        job_id = scrape_scheduler.trigger_background_scrape(
            mode_name=mode_name,
            user_id=str(mode_doc.get("user_id") or request.user.get("sub") or ""),
            mode_id=str(mode_doc["_id"]),
            scrape_sites=scrape_sites
        )

        # Update "last scrape" timestamp immediately when a run is initiated/queued
        modes_collection.update_one(
            {"_id": mode_doc["_id"]},
            {"$set": {"last_scraped_at": datetime.utcnow()}},
        )
        
        # Return immediately with job ID
        return {
            "status": "queued",
            "job_id": str(job_id),
            "mode_name": mode_name,
            "message": "Scraping started in background. Depending on the size of the site(s), this may take several minutes to complete.",
            "total_sites": len(scrape_sites)
        }, 202  # 202 Accepted
        
    except Exception as e:
        print(f"Error triggering scrape: {e}")
        return {"error": "Failed to trigger scraping", "details": str(e)}, 500


@routes.get("/admin/scrape/status/<mode_id>")
@cognito_auth_required
def get_scrape_status(mode_id):
    """Get scraping status and history for a mode."""
    try:
        mode_obj_id = ObjectId(mode_id)
    except Exception:  # noqa: BLE001
        return {"error": "Invalid mode id"}, 400

    mode_doc = modes_collection.find_one({"_id": mode_obj_id})
    if not mode_doc:
        return {"error": "Mode not found"}, 404
    if not request.user.get("is_super_admin"):
        if not mode_doc.get("user_id") or str(mode_doc.get("user_id")) != str(request.user.get("sub")):
            return {"error": "Access denied"}, 403
    
    mode_name = mode_doc.get("name")
    
    # Get scraped content for this mode (updated for new schema with modes array)
    content_docs = list(scraped_content_collection.find({"modes": mode_name}))
    
    scraped_content = []
    for doc in content_docs:
        scraped_content.append({
            "_id": str(doc["_id"]),
            "url": doc.get("original_url") or doc.get("url"),  # Support both old and new schema
            "title": doc.get("title", "Untitled"),
            "status": doc.get("status"),
            "scraped_at": doc.get("scraped_at").isoformat() if doc.get("scraped_at") else None,
            "error_message": doc.get("error_message"),
            "word_count": doc.get("metadata", {}).get("word_count", 0)
        })
    
    return {
        "mode": mode_name,
        "last_scraped_at": mode_doc.get("last_scraped_at").isoformat() if mode_doc.get("last_scraped_at") else None,
        "has_scraped_content": mode_doc.get("has_scraped_content", False),
        "scrape_frequency": mode_doc.get("scrape_frequency", "manual"),
        "configured_sites": mode_doc.get("scrape_sites", []),
        "scraped_content": scraped_content
    }


@routes.get("/admin/scrape/sites/<mode_id>")
@cognito_auth_required
def get_scraped_sites(mode_id):
    """Get all scraped sites (grouped by domain) for a mode."""
    try:
        try:
            mode_obj_id = ObjectId(mode_id)
        except Exception:  # noqa: BLE001
            return {"error": "Invalid mode id"}, 400

        mode_doc = modes_collection.find_one({"_id": mode_obj_id})
        if not mode_doc:
            return {"error": "Mode not found"}, 404
        if not request.user.get("is_super_admin"):
            if not mode_doc.get("user_id") or str(mode_doc.get("user_id")) != str(request.user.get("sub")):
                return {"error": "Access denied"}, 403
        
        mode_name = mode_doc.get("name")
        
        # Aggregate content by base_domain
        pipeline = [
            {"$match": {"modes": mode_name, "status": "active"}},
            {"$group": {
                "_id": "$base_domain",
                "total_pages": {"$sum": 1},
                "total_words": {"$sum": "$metadata.word_count"},
                "last_scraped": {"$max": "$scraped_at"},
                "sample_url": {"$first": "$original_url"}
            }},
            {"$sort": {"last_scraped": -1}}
        ]
        
        sites = list(scraped_content_collection.aggregate(pipeline))
        
        return {
            "sites": [{
                "domain": site["_id"],
                "total_pages": site["total_pages"],
                "total_words": site["total_words"],
                "last_scraped": site["last_scraped"].isoformat() if site.get("last_scraped") else None,
                "sample_url": site.get("sample_url")
            } for site in sites]
        }
    except Exception as e:
        print(f"Error getting scraped sites: {e}")
        return {"error": "Failed to get scraped sites", "details": str(e)}, 500


@routes.delete("/admin/scrape/site/<mode_id>/<domain>")
@cognito_auth_required
def delete_site_content(mode_id, domain):
    """Delete all scraped content from a specific site for a mode (runs in background)."""
    try:
        try:
            mode_obj_id = ObjectId(mode_id)
        except Exception:  # noqa: BLE001
            return {"error": "Invalid mode id"}, 400

        mode_doc = modes_collection.find_one({"_id": mode_obj_id})
        if not mode_doc:
            return {"error": "Mode not found"}, 404
        if not request.user.get("is_super_admin"):
            if not mode_doc.get("user_id") or str(mode_doc.get("user_id")) != str(request.user.get("sub")):
                return {"error": "Access denied"}, 403
        
        mode_name = mode_doc.get("name")
        
        # Find all content for this domain and mode
        content_docs = list(scraped_content_collection.find({
            "base_domain": domain,
            "modes": mode_name
        }))
        
        if not content_docs:
            return {"error": "No content found for this site"}, 404
        
        # Start background deletion job
        job_id = scraper_client.queue_site_delete(
            mode_id=mode_id,
            mode_name=mode_name,
            domain=domain,
            user_id=str(mode_doc.get("user_id") or request.user.get("sub") or ""),
            auto_dispatch=True
        )
        
        # Return immediately
        return {
            "status": "deleting",
            "job_id": str(job_id),
            "message": "Deletion started in background."
        }, 202  # 202 Accepted
        
    except Exception as e:
        print(f"Error starting site content deletion: {e}")
        return {"error": "Failed to start deletion", "details": str(e)}, 500


@routes.get("/admin/scrape/blocked-pages/<mode_id>")
@cognito_auth_required
def get_blocked_pages(mode_id):
    """List blocked page URLs for a mode (used by Mode Editor UI)."""
    try:
        try:
            mode_obj_id = ObjectId(mode_id)
        except Exception:  # noqa: BLE001
            return {"error": "Invalid mode id"}, 400

        mode_doc = modes_collection.find_one({"_id": mode_obj_id})
        if not mode_doc:
            return {"error": "Mode not found"}, 404

        mode_name = mode_doc.get("name")
        user_id = mode_doc.get("user_id")
        if not request.user.get("is_super_admin"):
            if not user_id or str(user_id) != str(request.user.get("sub")):
                return {"error": "Access denied"}, 403

        docs = list(
            blocked_pages_collection.find(
                {"mode": mode_name, "user_id": user_id},
                {"_id": 0, "mode": 0, "user_id": 0},
            ).sort("blocked_at", -1)
        )
        # Ensure consistent fields
        blocked_pages = []
        for d in docs:
            blocked_pages.append(
                {
                    "normalized_url": d.get("normalized_url"),
                    "url": d.get("url") or d.get("original_url"),
                    "title": d.get("title") or "Blocked Page",
                    "blocked_at": d.get("blocked_at").isoformat() if d.get("blocked_at") else None,
                }
            )

        return {
            "mode_id": mode_id,
            "mode": mode_name,
            "blocked_pages": blocked_pages,
            "count": len(blocked_pages),
        }, 200
    except Exception as e:  # noqa: BLE001
        print(f"Error fetching blocked pages: {e}")
        return {"error": "Failed to fetch blocked pages", "details": str(e)}, 500


@routes.post("/admin/scrape/block-page/<mode_id>")
@cognito_auth_required
def block_page(mode_id):
    """Block a scraped page URL for a mode and remove the page content."""
    data = request.get_json(silent=True) or {}
    content_id = (data.get("content_id") or "").strip()
    url = (data.get("url") or "").strip()
    normalized_url = (data.get("normalized_url") or "").strip()

    if not content_id and not (url or normalized_url):
        return {"error": "content_id or url is required"}, 400

    try:
        mode_obj_id = ObjectId(mode_id)
    except Exception:  # noqa: BLE001
        return {"error": "Invalid mode id"}, 400

    mode_doc = modes_collection.find_one({"_id": mode_obj_id})
    if not mode_doc:
        return {"error": "Mode not found"}, 404
    mode_name = mode_doc.get("name")
    mode_owner_id = mode_doc.get("user_id")
    if not request.user.get("is_super_admin"):
        if not mode_owner_id or str(mode_owner_id) != str(request.user.get("sub")):
            _log_access_denied(
                "block_page_mode_owner_mismatch",
                mode_id=mode_id,
                mode_owner_id=mode_owner_id,
            )
            return {"error": "Access denied"}, 403

    # Resolve from content_id when possible (preferred, gives normalized_url)
    scraped_doc = None
    if content_id:
        try:
            content_obj_id = ObjectId(content_id)
        except Exception:  # noqa: BLE001
            return {"error": "Invalid content id"}, 400

        scraped_doc = scraped_content_collection.find_one({"_id": content_obj_id})
        if not scraped_doc:
            return {"error": "Content not found"}, 404
        if mode_name not in (scraped_doc.get("modes") or []):
            return {"error": "Content does not belong to this mode"}, 400

        normalized_url = scraped_doc.get("normalized_url") or normalized_url
        url = scraped_doc.get("original_url") or scraped_doc.get("url") or url

    if not normalized_url:
        # Fall back to a best-effort normalization using the scraped_content document
        # or treat original url as normalized if unavailable.
        normalized_url = url

    # Store in mode doc for fast skip during scraping
    modes_collection.update_one(
        {"_id": mode_obj_id},
        {"$addToSet": {"blocked_page_urls": normalized_url}},
    )

    # Store display metadata in dedicated collection
    now = datetime.utcnow()
    try:
        blocked_pages_collection.update_one(
            {"mode": mode_name, "user_id": mode_owner_id, "normalized_url": normalized_url},
            {
                "$setOnInsert": {"blocked_at": now},
                "$set": {"url": url, "title": (scraped_doc or {}).get("title") or None, "updated_at": now},
            },
            upsert=True,
        )
    except Exception as e:  # noqa: BLE001
        print(f"Failed to upsert blocked page record: {e}")

    # IMPORTANT: Blocking is per-mode. Detach the page from THIS mode without impacting other modes.
    # Only delete the underlying scraped_content document if it is no longer used by any mode.
    job_id = None
    try:
        if content_id and scraped_doc:
            updated = scraped_content_collection.find_one_and_update(
                {"_id": content_obj_id},
                {"$pull": {"modes": mode_name}},
                return_document=ReturnDocument.AFTER,
            )
            if updated and not (updated.get("modes") or []):
                try:
                    job_id = scraper_client.queue_delete_content(
                        content_id=content_id,
                        user_id=str(request.user.get("sub") or ""),
                        mode_name=None,
                    )
                except Exception as e:  # noqa: BLE001
                    print(f"Failed to queue delete job for orphaned blocked page: {e}")
        elif normalized_url:
            # Best-effort detach when the UI blocks by URL (no content_id provided)
            scraped_content_collection.update_many(
                {"normalized_url": normalized_url, "modes": mode_name},
                {"$pull": {"modes": mode_name}},
            )
    except Exception as e:  # noqa: BLE001
        print(f"Failed to detach blocked page from mode: {e}")

    return {"status": "blocked", "normalized_url": normalized_url, "job_id": str(job_id) if job_id else None}, 200


@routes.post("/admin/scrape/block-pages/<mode_id>")
@cognito_auth_required
def block_pages_bulk(mode_id):
    """Bulk block multiple scraped pages for a mode and remove them."""
    data = request.get_json(silent=True) or {}
    content_ids = data.get("content_ids") or []
    if not isinstance(content_ids, list) or not content_ids:
        return {"error": "content_ids is required"}, 400

    try:
        mode_obj_id = ObjectId(mode_id)
    except Exception:  # noqa: BLE001
        return {"error": "Invalid mode id"}, 400

    mode_doc = modes_collection.find_one({"_id": mode_obj_id})
    if not mode_doc:
        return {"error": "Mode not found"}, 404
    mode_name = mode_doc.get("name")
    mode_owner_id = mode_doc.get("user_id")
    if not request.user.get("is_super_admin"):
        if not mode_owner_id or str(mode_owner_id) != str(request.user.get("sub")):
            _log_access_denied(
                "block_pages_bulk_mode_owner_mismatch",
                mode_id=mode_id,
                mode_owner_id=mode_owner_id,
                extra={"count": len(content_ids) if isinstance(content_ids, list) else None},
            )
            return {"error": "Access denied"}, 403

    try:
        content_obj_ids = [ObjectId(cid) for cid in content_ids]
    except Exception:  # noqa: BLE001
        return {"error": "Invalid content id in content_ids"}, 400

    scraped_docs = list(scraped_content_collection.find({"_id": {"$in": content_obj_ids}}))
    if not scraped_docs:
        return {"error": "No content found"}, 404

    normalized_urls = []
    records = []
    now = datetime.utcnow()
    valid_content_ids = []

    for d in scraped_docs:
        if mode_name not in (d.get("modes") or []):
            continue
        nurl = d.get("normalized_url") or (d.get("original_url") or d.get("url"))
        ourl = d.get("original_url") or d.get("url")
        if not nurl:
            continue
        normalized_urls.append(nurl)
        valid_content_ids.append(str(d["_id"]))
        records.append(
            {
                "mode": mode_name,
                "user_id": mode_owner_id,
                "normalized_url": nurl,
                "url": ourl,
                "title": d.get("title") or None,
                "blocked_at": now,
                "updated_at": now,
            }
        )

    if not normalized_urls:
        return {"error": "No eligible pages to block"}, 400

    modes_collection.update_one(
        {"_id": mode_obj_id},
        {"$addToSet": {"blocked_page_urls": {"$each": normalized_urls}}},
    )

    try:
        if records:
            blocked_pages_collection.insert_many(records, ordered=False)
    except Exception:
        # Ignore duplicates/insert issues (we also store list in mode doc)
        pass

    # IMPORTANT: per-mode block. Detach each page from THIS mode; delete only if orphaned.
    job_ids = []
    for cid in valid_content_ids:
        try:
            content_obj_id = ObjectId(cid)
            updated = scraped_content_collection.find_one_and_update(
                {"_id": content_obj_id},
                {"$pull": {"modes": mode_name}},
                return_document=ReturnDocument.AFTER,
            )
            if updated and not (updated.get("modes") or []):
                try:
                    job_id = scraper_client.queue_delete_content(
                        content_id=cid,
                        user_id=str(request.user.get("sub") or ""),
                        mode_name=None,
                    )
                    job_ids.append(str(job_id))
                except Exception as e:  # noqa: BLE001
                    print(f"Failed to queue delete for orphaned content {cid}: {e}")
        except Exception as e:  # noqa: BLE001
            print(f"Failed to detach block for {cid}: {e}")

    return {"status": "blocked", "count": len(normalized_urls), "job_ids": job_ids}, 200


@routes.post("/admin/scrape/unblock-page/<mode_id>")
@cognito_auth_required
def unblock_page(mode_id):
    """Unblock a page URL for a mode."""
    data = request.get_json(silent=True) or {}
    normalized_url = (data.get("normalized_url") or "").strip()
    if not normalized_url:
        return {"error": "normalized_url is required"}, 400

    try:
        mode_obj_id = ObjectId(mode_id)
    except Exception:  # noqa: BLE001
        return {"error": "Invalid mode id"}, 400

    mode_doc = modes_collection.find_one({"_id": mode_obj_id})
    if not mode_doc:
        return {"error": "Mode not found"}, 404
    mode_name = mode_doc.get("name")
    mode_owner_id = mode_doc.get("user_id")
    if not request.user.get("is_super_admin"):
        if not mode_owner_id or str(mode_owner_id) != str(request.user.get("sub")):
            return {"error": "Access denied"}, 403

    modes_collection.update_one(
        {"_id": mode_obj_id},
        {"$pull": {"blocked_page_urls": normalized_url}},
    )
    blocked_pages_collection.delete_one({"mode": mode_name, "user_id": mode_owner_id, "normalized_url": normalized_url})
    return {"status": "unblocked", "normalized_url": normalized_url}, 200


@routes.post("/admin/scrape/unblock-pages/<mode_id>")
@cognito_auth_required
def unblock_pages_bulk(mode_id):
    """Bulk unblock page URLs for a mode."""
    data = request.get_json(silent=True) or {}
    normalized_urls = data.get("normalized_urls") or []
    if not isinstance(normalized_urls, list) or not normalized_urls:
        return {"error": "normalized_urls is required"}, 400

    try:
        mode_obj_id = ObjectId(mode_id)
    except Exception:  # noqa: BLE001
        return {"error": "Invalid mode id"}, 400

    mode_doc = modes_collection.find_one({"_id": mode_obj_id})
    if not mode_doc:
        return {"error": "Mode not found"}, 404
    mode_name = mode_doc.get("name")
    mode_owner_id = mode_doc.get("user_id")
    if not request.user.get("is_super_admin"):
        if not mode_owner_id or str(mode_owner_id) != str(request.user.get("sub")):
            return {"error": "Access denied"}, 403

    modes_collection.update_one(
        {"_id": mode_obj_id},
        {"$pullAll": {"blocked_page_urls": normalized_urls}},
    )
    blocked_pages_collection.delete_many({"mode": mode_name, "user_id": mode_owner_id, "normalized_url": {"$in": normalized_urls}})
    return {"status": "unblocked", "count": len(normalized_urls)}, 200


@routes.get("/admin/scrape/discovered-files/<mode_id>")
@cognito_auth_required
def get_discovered_files(mode_id):
    """Get all discovered downloadable files for a mode."""
    try:
        try:
            mode_obj_id = ObjectId(mode_id)
        except Exception:  # noqa: BLE001
            return {"error": "Invalid mode id"}, 400

        mode_doc = modes_collection.find_one({"_id": mode_obj_id})
        if not mode_doc:
            return {"error": "Mode not found"}, 404
        if not request.user.get("is_super_admin"):
            if not mode_doc.get("user_id") or str(mode_doc.get("user_id")) != str(request.user.get("sub")):
                return {"error": "Access denied"}, 403
        
        mode_name = mode_doc.get("name")
        
        # Get discovered files collection
        discovered_files_collection = db.get_collection("discovered_files")
        
        # Find all discovered files for this mode
        files = list(discovered_files_collection.find({
            "mode": mode_name,
            "status": "discovered"
        }).sort("discovered_at", -1))
        
        # Convert ObjectId to string
        for file in files:
            file["_id"] = str(file["_id"])
            if "discovered_at" in file:
                file["discovered_at"] = file["discovered_at"].isoformat()
        
        return {
            "files": files,
            "total": len(files)
        }, 200
        
    except Exception as e:
        print(f"Error fetching discovered files: {e}")
        return {"error": "Failed to fetch discovered files", "details": str(e)}, 500


@routes.post("/admin/scrape/add-file/<mode_id>")
@cognito_auth_required
def add_discovered_file(mode_id):
    """Download and add a discovered file to the mode's documents."""
    try:
        try:
            mode_obj_id = ObjectId(mode_id)
        except Exception:  # noqa: BLE001
            return {"error": "Invalid mode id"}, 400

        mode_doc = modes_collection.find_one({"_id": mode_obj_id})
        if not mode_doc:
            return {"error": "Mode not found"}, 404
        if not request.user.get("is_super_admin"):
            if not mode_doc.get("user_id") or str(mode_doc.get("user_id")) != str(request.user.get("sub")):
                return {"error": "Access denied"}, 403
        
        mode_name = mode_doc.get("name")
        data = request.get_json()
        file_id = data.get("file_id")
        tag = data.get("tag", "").strip()
        
        if not file_id:
            return {"error": "file_id is required"}, 400
        
        # Get the discovered file record
        discovered_files_collection = db.get_collection("discovered_files")
        file_doc = discovered_files_collection.find_one({"_id": ObjectId(file_id)})
        
        if not file_doc:
            return {"error": "File not found"}, 404
        
        if file_doc.get("mode") != mode_name:
            return {"error": "File does not belong to this mode"}, 403

        if not request.user.get("is_super_admin"):
            if not file_doc.get("user_id") or str(file_doc.get("user_id")) != str(mode_doc.get("user_id")):
                return {"error": "Access denied"}, 403
        
        # Check if already added
        if file_doc.get("status") == "added":
            return {"error": "File already added to mode documents"}, 400
        
        file_url = file_doc.get("file_url")
        filename = file_doc.get("filename")
        
        # Download the file from URL
        print(f"Downloading file from {file_url}...")
        response = requests.get(file_url, timeout=60)
        response.raise_for_status()
        file_data = response.content
        
        # Determine content type
        content_type = response.headers.get('Content-Type', 'application/octet-stream')
        
        # Upload to S3
        s3_key = f"{mode_name}/{tag or 'untagged'}/{filename}"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=file_data,
            ContentType=content_type,
            Metadata={}
        )
        print(f"Uploaded to S3: {s3_key}")
        
        # Upload to OpenAI
        file_stream = io.BytesIO(file_data)
        openai_file = client.files.create(file=(filename, file_stream), purpose="assistants")
        openai_file_id = openai_file.id
        print(f"Uploaded to OpenAI: {openai_file_id}")
        
        # Add to vector store
        if VECTOR_STORE_ID:
            try:
                vs_meta = {"mode": mode_name}
                if tag:
                    vs_meta["tag"] = tag
                client.vector_stores.files.create(
                    vector_store_id=VECTOR_STORE_ID,
                    file_id=openai_file_id,
                    attributes=vs_meta
                )
                print(f"Added to vector store: {VECTOR_STORE_ID}")
            except Exception as e:
                print(f"Vector store add failed: {e}")
        
        # Save to MongoDB documents collection
        doc = {
            "user_id": mode_doc.get("user_id"),
            "mode": mode_name,
            "content": "",
            "tag": tag,
            "s3_key": s3_key,
            "openai_file_id": openai_file_id,
            "always_include": False,
            "source": "discovered",
            "source_url": file_url,
            "filename": filename
        }
        result = documents_collection.insert_one(doc)
        print(f"Saved to MongoDB documents collection")
        
        # Update the discovered file status
        discovered_files_collection.update_one(
            {"_id": ObjectId(file_id)},
            {
                "$set": {
                    "status": "added",
                    "added_at": datetime.utcnow(),
                    "document_id": str(result.inserted_id)
                }
            }
        )
        
        # Update mode's has_files flag
        modes_collection.update_one(
            {"name": mode_name},
            {"$set": {"has_files": True}}
        )
        
        return {
            "success": True,
            "message": "File successfully added to mode documents",
            "document_id": str(result.inserted_id),
            "openai_file_id": openai_file_id
        }, 200
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return {"error": "Failed to download file", "details": str(e)}, 500
    except Exception as e:
        print(f"Error adding discovered file: {e}")
        return {"error": "Failed to add file", "details": str(e)}, 500


@routes.post("/admin/scrape/block-file/<mode_id>")
@cognito_auth_required
def block_discovered_file(mode_id):
    """Block a discovered file URL and remove it from the list."""
    try:
        data = request.get_json()
        file_id = data.get("file_id")
        
        if not file_id:
            return {"error": "file_id is required"}, 400
        
        # Get the discovered file
        discovered_files_collection = db.get_collection("discovered_files")
        file_doc = discovered_files_collection.find_one({"_id": ObjectId(file_id)})
        
        if not file_doc:
            return {"error": "File not found"}, 404
        
        file_url = file_doc.get("file_url")
        mode_name = file_doc.get("mode")
        if not file_url:
            return {"error": "File URL not found"}, 400
        if not mode_name:
            return {"error": "File mode not found"}, 400
        
        # Verify mode exists + access (do NOT bake user_id into the Mongo query; it can be
        # stored as a non-string in older data, which causes false 403s.)
        try:
            mode_obj_id = ObjectId(mode_id)
        except Exception:  # noqa: BLE001
            return {"error": "Invalid mode id"}, 400

        mode_doc = modes_collection.find_one({"_id": mode_obj_id})
        if not mode_doc:
            return {"error": "Mode not found"}, 404

        if not request.user.get("is_super_admin"):
            owner_id = mode_doc.get("user_id")
            if not owner_id or str(owner_id) != str(request.user.get("sub")):
                return {"error": "Access denied"}, 403
            
        if mode_doc.get("name") != mode_name:
            return {"error": "File does not belong to this mode"}, 400
            
        # Add URL to blocked_file_urls
        modes_collection.update_one(
            {"_id": mode_obj_id},
            {"$addToSet": {"blocked_file_urls": file_url}}
        )
        
        # Delete the discovered file record
        discovered_files_collection.delete_one({"_id": ObjectId(file_id)})
        
        print(f"Blocked file URL: {file_url} for mode {mode_name}")
        
        return {
            "success": True,
            "message": "File blocked and removed from list"
        }, 200
        
    except Exception as e:
        print(f"Error blocking file: {e}")
        return {"error": "Failed to block file", "details": str(e)}, 500


@routes.delete("/admin/scrape/discovered-file/<file_id>")
@cognito_auth_required
def delete_discovered_file(file_id):
    """Delete a discovered file from the list."""
    try:
        # Get the discovered file
        discovered_files_collection = db.get_collection("discovered_files")
        try:
            file_obj_id = ObjectId(file_id)
        except Exception:  # noqa: BLE001
            return {"error": "Invalid file id"}, 400

        file_doc = discovered_files_collection.find_one({"_id": file_obj_id})
        
        if not file_doc:
            return {"error": "File not found"}, 404
        
        mode_name = file_doc.get("mode")
        
        # Verify user has access (string-safe check; older data may store ids as non-strings)
        if not request.user.get("is_super_admin"):
            file_owner_id = file_doc.get("user_id")
            if not file_owner_id or str(file_owner_id) != str(request.user.get("sub")):
                return {"error": "Access denied"}, 403
        
        # Delete the discovered file record
        discovered_files_collection.delete_one({"_id": file_obj_id})
        
        print(f"Deleted discovered file: {file_doc.get('filename')} from mode {mode_name}")
        
        return {
            "success": True,
            "message": "File removed from discovered list"
        }, 200
        
    except Exception as e:
        print(f"Error deleting discovered file: {e}")
        return {"error": "Failed to delete file", "details": str(e)}, 500


@routes.get("/admin/scraped-content")
@cognito_auth_required
def list_scraped_content():
    """List all scraped content, optionally filtered by mode."""
    mode = request.args.get("mode", "").strip()

    if request.user.get("is_super_admin"):
        query = {}
        if mode:
            query["modes"] = mode  # Updated for new schema with modes array
        content_docs = list(scraped_content_collection.find(query))
    else:
        pipeline = [{"$match": {"$expr": {"$eq": [{"$toString": "$user_id"}, str(request.user.get("sub"))]}}}]
        if mode:
            pipeline.insert(0, {"$match": {"modes": mode}})
        content_docs = list(scraped_content_collection.aggregate(pipeline))
    
    scraped_content = []
    for doc in content_docs:
        # Get modes list (support both old and new schema)
        modes_list = doc.get("modes", [])
        if not modes_list and doc.get("mode"):
            modes_list = [doc.get("mode")]
        
        scraped_content.append({
            "_id": str(doc["_id"]),
            "mode": doc.get("mode") or (modes_list[0] if modes_list else None),  # Backward compat
            "modes": modes_list,  # New field with all modes
            "url": doc.get("original_url") or doc.get("url"),  # Support both schemas
            "title": doc.get("title", "Untitled"),
            "status": doc.get("status"),
            "scraped_at": doc.get("scraped_at").isoformat() if doc.get("scraped_at") else None,
            "error_message": doc.get("error_message"),
            "word_count": doc.get("metadata", {}).get("word_count", 0)
        })
    
    return {"scraped_content": scraped_content}


@routes.delete("/admin/scraped-content/<content_id>")
@cognito_auth_required
def delete_scraped_content(content_id):
    """Delete scraped content (runs in background)."""
    try:
        content_obj_id = ObjectId(content_id)
    except Exception:  # noqa: BLE001
        return {"error": "Invalid content id"}, 400

    doc = scraped_content_collection.find_one({"_id": content_obj_id})
    if not doc:
        return {"error": "Content not found"}, 404
    if not request.user.get("is_super_admin"):
        if not doc.get("user_id") or str(doc.get("user_id")) != str(request.user.get("sub")):
            return {"error": "Access denied"}, 403
    
    job_id = scraper_client.queue_delete_content(
        content_id=content_id,
        user_id=request.user["sub"],
        mode_name=None,
    )
    
    if not scraper_client.is_remote and SCRAPER_EXECUTION_MODE == "local":
        message = "Deletion running on local scraper service"
    else:
        message = "Deletion job queued on scraper worker"
    
    return {
        "status": "queued",
        "message": message,
        "job_id": str(job_id)
    }, 202  # 202 Accepted


@routes.post("/admin/scrape/refresh/<content_id>")
@cognito_auth_required
def refresh_scraped_content(content_id):
    """Re-scrape a specific URL."""
    try:
        content_obj_id = ObjectId(content_id)
    except Exception:  # noqa: BLE001
        return {"error": "Invalid content id"}, 400

    doc = scraped_content_collection.find_one({"_id": content_obj_id})
    if not doc:
        return {"error": "Content not found"}, 404
    if not request.user.get("is_super_admin"):
        if not doc.get("user_id") or str(doc.get("user_id")) != str(request.user.get("sub")):
            return {"error": "Access denied"}, 403
    
    # Support both old and new schema
    url = doc.get("original_url") or doc.get("url")
    modes_list = doc.get("modes", [])
    mode = modes_list[0] if modes_list else doc.get("mode")
    user_id = doc.get("user_id")
    normalized_url = doc.get("normalized_url") or url
    
    if not url or not mode:
        return {"error": "Invalid content document"}, 400

    # Block check: prevent refresh if URL is blocked for this mode
    mode_doc_for_owner = None
    try:
        for candidate in modes_collection.find({"name": mode}, {"blocked_page_urls": 1, "user_id": 1}):
            if str(candidate.get("user_id")) == str(user_id):
                mode_doc_for_owner = candidate
                break
        if mode_doc_for_owner and normalized_url and normalized_url in (mode_doc_for_owner.get("blocked_page_urls") or []):
            return {"error": "This page URL is blocked for this mode"}, 400
    except Exception as e:  # noqa: BLE001
        print(f"Blocked-page check failed: {e}")
    
    if SCRAPER_EXECUTION_MODE != "local" or scraping_service is None:
        job_id = scraper_client.queue_single_url_refresh(
            content_id=content_id,
            url=url,
            mode_name=mode,
            user_id=user_id,
        )

        # Update mode "last scrape" timestamp when a refresh is initiated/queued
        try:
            if mode_doc_for_owner and mode_doc_for_owner.get("_id"):
                modes_collection.update_one(
                    {"_id": mode_doc_for_owner["_id"]},
                    {"$set": {"last_scraped_at": datetime.utcnow()}},
                )
        except Exception as e:  # noqa: BLE001
            print(f"Error updating last_scraped_at for mode refresh: {e}")

        return {
            "status": "queued",
            "job_id": str(job_id),
            "mode": mode,
            "message": "Refresh job enqueued on scraper worker"
        }, 202
    
    try:
        # Scrape the URL locally
        content, title, error = scraping_service.scrape_url(url)
        
        if error:
            # Update with error
            scraped_content_collection.update_one(
                {"_id": ObjectId(content_id)},
                {
                    "$set": {
                        "status": "failed",
                        "error_message": error,
                        "scraped_at": datetime.utcnow()
                    }
                }
            )
            return {"error": error}, 400
        
        # Upload to vector store
        scraped_at = datetime.utcnow()
        
        # Delete old file if exists
        old_file_id = doc.get("openai_file_id")
        if old_file_id and VECTOR_STORE_ID:
            try:
                client.files.delete(old_file_id)
                client.vector_stores.files.delete(
                    vector_store_id=VECTOR_STORE_ID,
                    file_id=old_file_id
                )
            except Exception as e:
                print(f"Error deleting old file: {e}")
        
        openai_file_id = scraping_service.upload_to_vector_store(
            content, mode, url, title, scraped_at
        )

        # Update mode "last scrape" timestamp after a successful refresh run
        try:
            if mode_doc_for_owner and mode_doc_for_owner.get("_id"):
                modes_collection.update_one(
                    {"_id": mode_doc_for_owner["_id"]},
                    {"$set": {"last_scraped_at": scraped_at}},
                )
        except Exception as e:  # noqa: BLE001
            print(f"Error updating last_scraped_at after local refresh: {e}")
        
        # Update document
        scraped_content_collection.update_one(
            {"_id": ObjectId(content_id)},
            {
                "$set": {
                    "title": title,
                    "content": content,
                    "scraped_at": scraped_at,
                    "openai_file_id": openai_file_id,
                    "status": "active",
                    "error_message": None,
                    "metadata": {
                        "word_count": len(content.split()),
                        "char_count": len(content)
                    }
                }
            }
        )
        
        return {
            "status": "success",
            "title": title,
            "word_count": len(content.split())
        }, 200
        
    except Exception as e:
        print(f"Error refreshing content: {e}")
        return {"error": "Failed to refresh content", "details": str(e)}, 500


@routes.get("/admin/scrape/job/<job_id>")
@cognito_auth_required
def get_scrape_job_status(job_id):
    """Get the status of a specific scraping job."""
    try:
        try:
            job_obj_id = ObjectId(job_id)
        except Exception:  # noqa: BLE001
            return {"error": "Invalid job id"}, 400

        job = scraping_jobs_collection.find_one({"_id": job_obj_id})
        if not job:
            return {"error": "Job not found"}, 404
        if not request.user.get("is_super_admin"):
            if not job.get("user_id") or str(job.get("user_id")) != str(request.user.get("sub")):
                return {"error": "Access denied"}, 403
        
        return {
            "_id": str(job["_id"]),
            "mode_id": job.get("mode_id"),
            "mode_name": job["mode_name"],
            "status": job["status"],
            "progress": job.get("progress", {}),
            "result": job.get("result"),
            "error": job.get("error"),
            "created_at": job["created_at"].isoformat() if job.get("created_at") else None,
            "started_at": job["started_at"].isoformat() if job.get("started_at") else None,
            "completed_at": job["completed_at"].isoformat() if job.get("completed_at") else None
        }
    except Exception as e:
        print(f"Error getting job status: {e}")
        return {"error": "Failed to get job status", "details": str(e)}, 500


@routes.get("/admin/scrape/jobs")
@cognito_auth_required
def list_scrape_jobs():
    """List all scraping jobs for the user (recent first)."""
    try:
        mode_name = request.args.get("mode", "").strip()

        if request.user.get("is_super_admin"):
            query = {}
            if mode_name:
                query["mode_name"] = mode_name
            jobs = list(scraping_jobs_collection.find(query).sort("created_at", -1).limit(50))
        else:
            pipeline = [{"$match": {"$expr": {"$eq": [{"$toString": "$user_id"}, str(request.user.get("sub"))]}}}]
            if mode_name:
                pipeline.insert(0, {"$match": {"mode_name": mode_name}})
            pipeline.extend([{"$sort": {"created_at": -1}}, {"$limit": 50}])
            jobs = list(scraping_jobs_collection.aggregate(pipeline))
        
        return {
            "jobs": [{
                "_id": str(job["_id"]),
                "mode_id": job.get("mode_id"),
                "mode_name": job["mode_name"],
                "status": job["status"],
                "progress": job.get("progress", {}),
                "error": job.get("error"),
                "created_at": job["created_at"].isoformat() if job.get("created_at") else None,
                "started_at": job["started_at"].isoformat() if job.get("started_at") else None,
                "completed_at": job["completed_at"].isoformat() if job.get("completed_at") else None
            } for job in jobs]
        }
    except Exception as e:
        print(f"Error listing jobs: {e}")
        return {"error": "Failed to list jobs", "details": str(e)}, 500


@routes.get("/admin/scrape/active-jobs/<mode_id>")
@cognito_auth_required
def get_active_scrape_jobs(mode_id):
    """Get all active (in_progress or queued) scraping jobs for a specific mode."""
    try:
        # Get the mode to verify access
        try:
            mode_obj_id = ObjectId(mode_id)
        except Exception:  # noqa: BLE001
            return {"error": "Invalid mode id"}, 400

        mode = modes_collection.find_one({"_id": mode_obj_id})
        if not mode:
            return {"error": "Mode not found"}, 404
        if not request.user.get("is_super_admin"):
            if not mode.get("user_id") or str(mode.get("user_id")) != str(request.user.get("sub")):
                return {"error": "Access denied"}, 403
        
        # Find all active jobs for this mode
        query = {
            "mode_id": mode_id,
            "status": {"$in": ["queued", "in_progress"]}
        }
        
        jobs = list(scraping_jobs_collection.find(query).sort("created_at", -1))
        
        return {
            "jobs": [{
                "_id": str(job["_id"]),
                "job_type": job.get("job_type", "scrape"),
                "mode_id": job.get("mode_id"),
                "mode_name": job["mode_name"],
                "domain": job.get("domain"),
                "status": job["status"],
                "progress": job.get("progress", {}),
                "created_at": job["created_at"].isoformat() if job.get("created_at") else None,
                "started_at": job["started_at"].isoformat() if job.get("started_at") else None
            } for job in jobs]
        }
    except Exception as e:
        print(f"Error getting active jobs: {e}")
        return {"error": "Failed to get active jobs", "details": str(e)}, 500


@routes.delete("/admin/scrape/job/<job_id>")
@cognito_auth_required
def delete_scrape_job(job_id):
    """Delete a scraping job record (does not stop running jobs)."""
    try:
        try:
            job_obj_id = ObjectId(job_id)
        except Exception:  # noqa: BLE001
            return {"error": "Invalid job id"}, 400

        job = scraping_jobs_collection.find_one({"_id": job_obj_id})
        if not job:
            return {"error": "Job not found"}, 404
        if not request.user.get("is_super_admin"):
            if not job.get("user_id") or str(job.get("user_id")) != str(request.user.get("sub")):
                return {"error": "Access denied"}, 403

        result = scraping_jobs_collection.delete_one({"_id": job_obj_id})
        
        if result.deleted_count == 0:
            return {"error": "Job not found"}, 404
        
        return {"status": "deleted"}, 200
    except Exception as e:
        print(f"Error deleting job: {e}")
        return {"error": "Failed to delete job", "details": str(e)}, 500


@routes.route("/")
def index():
    return send_from_directory(routes.static_folder, "index.html")

app = Flask(__name__, static_folder="public", static_url_path="")
if localDevMode == "true":
    app.register_blueprint(routes, url_prefix="/flask")
else:
    app.register_blueprint(routes)

if __name__ == "__main__":
    # Start the scrape scheduler
    scrape_scheduler.start()
    
    port = int(os.getenv("PORT", "3000"))
    print(f"Starting server on port {port}")
    
    try:
        app.run(port=port)
    finally:
        # Stop the scheduler on shutdown
        scrape_scheduler.stop()