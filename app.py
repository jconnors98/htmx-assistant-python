import os
import re
import io
import json
from functools import wraps
from flask import Flask, Blueprint, request, send_from_directory
from markdown import markdown
import bleach
from decouple import config
from openai import OpenAI
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from bson import ObjectId
import boto3
import requests
from jose import jwk, jwt
from datetime import datetime, timedelta
import hashlib
import threading
from conversation_service import ConversationService

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

conversation_service = ConversationService(
    db,
    modes_collection,
    client,
    VECTOR_STORE_ID,
)

print("Connected to MongoDB at", config("MONGO_URI"))

localDevMode = config("LOCAL_DEV_MODE", default="false").lower()

AWS_ACCESS_KEY_ID = config("AWS_ACCESS_KEY_ID", default=None)
AWS_SECRET_ACCESS_KEY = config("AWS_SECRET_ACCESS_KEY", default=None)
COGNITO_REGION = config("COGNITO_REGION", default=None)
COGNITO_USER_POOL_ID = config("COGNITO_USER_POOL_ID", default=None)
COGNITO_APP_CLIENT_ID = config("COGNITO_APP_CLIENT_ID", default=None)

DEFAULT_MODE_COLOR = "#82002d"
DEFAULT_TEXT_COLOR = "#ffffff"

S3_BUCKET = config("S3_BUCKET", default="builders-copilot")
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=COGNITO_REGION
)
_jwks = None


def _get_priority_source(data, default="sites"):
    if not data:
        return default

    preference = data.get("priority_source")
    if isinstance(preference, str):
        preference = preference.strip().lower()
    if preference in {"sites", "files"}:
        return preference

    if data.get("prioritize_files"):
        return "files"

    return default


def _get_jwks():
    global _jwks
    if _jwks is None and COGNITO_REGION and COGNITO_USER_POOL_ID:
        url = (
            f"https://cognito-idp.{COGNITO_REGION}.amazonaws.com/"
            f"{COGNITO_USER_POOL_ID}/.well-known/jwks.json"
        )
        _jwks = requests.get(url, timeout=30).json().get("keys", [])
    return _jwks or []


def _is_super_admin(user_id):
    if not user_id:
        return False

    or_clauses = [{"user_id": user_id}]
    try:
        or_clauses.append({"_id": ObjectId(user_id)})
    except Exception:  # noqa: BLE001
        or_clauses.append({"_id": user_id})

    return (
        superadmins_collection.count_documents({"$or": or_clauses}, limit=1) > 0
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
            "is_super_admin": _is_super_admin(user_id),
        }
        return fn(*args, **kwargs)

    return wrapper


routes = Blueprint("routes", __name__, static_folder='public', static_url_path='')

@routes.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Content-Security-Policy"] = "frame-ancestors *"
    return response

def _async_log_prompt(prompt=None, response=None, mode=None, ip_addr=None, conversation_id=None, response_id=None):
    ip_hash = hashlib.sha256(ip_addr.encode()).hexdigest() if ip_addr else None
    location = {}
    mode = str(mode) if mode else "<unknown>"
    
    log_type = "response" if response else "prompt"
    print(f"Logging {log_type} from IP: {ip_addr}")
    
    if ip_addr:
        try:
            resp = requests.get(f"https://ipapi.co/{ip_addr}/json/", timeout=30)
            print(f"IP API response: {resp.status_code} {resp.text}")
            if resp.ok:
                data = resp.json()
                location = {
                    "city": data.get("city"),
                    "region": data.get("region"),
                    "country": data.get("country_name"),
                }
        except Exception as e:
            print(f"Error fetching IP info: {e}")
            pass
    else:
        print("No IP address found.")

    log_entry = {
        "mode": mode,
        "conversation_id": conversation_id,
        "ip_hash": ip_hash,
        "location": location,
        "created_at": datetime.utcnow(),
    }
    
    if prompt is not None:
        log_entry["prompt"] = prompt
    if response is not None:
        log_entry["response"] = response
    if response_id is not None:
        log_entry["response_id"] = response_id

    prompt_logs_collection.insert_one(log_entry)

def _parse_date(value, end=False):
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d")
        except ValueError:
            return None
    if end and parsed.time() == datetime.min.time():
        parsed = parsed + timedelta(days=1) - timedelta(microseconds=1)
    return parsed

def _normalize_color(value):
    if not value:
        return DEFAULT_MODE_COLOR

    text = str(value).strip()
    if not text:
        return DEFAULT_MODE_COLOR

    if not text.startswith("#"):
        text = f"#{text}"

    if not re.fullmatch(r"#[0-9a-fA-F]{6}", text):
        return DEFAULT_MODE_COLOR

    return text.lower()


def _normalize_text_color(value):
    if not value:
        return DEFAULT_TEXT_COLOR

    text = str(value).strip()
    if not text:
        return DEFAULT_TEXT_COLOR

    if not text.startswith("#"):
        text = f"#{text}"

    if not re.fullmatch(r"#[0-9a-fA-F]{6}", text):
        return DEFAULT_TEXT_COLOR

    return text.lower()


@routes.post("/upload-files")
def upload_files():
    files = request.files.getlist("files")
    file_ids = []
    
    for file in files:
        if file and file.filename:
            data = file.read()
            file_stream = io.BytesIO(data)
            uploaded = client.files.create(
                file=(file.filename, file_stream), purpose="assistants"
            )
            file_ids.append(uploaded.id)
    
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
        doc["color"] = _normalize_color(doc.get("color"))
        doc["text_color"] = _normalize_text_color(doc.get("text_color"))
    return {"modes": docs}


@routes.get("/modes/<mode>")
def get_mode(mode):
    doc = modes_collection.find_one({"name": mode}, {"_id": 0, "database": 0})
    if not doc:
        return {"prompts": []}, 404
    return {
        "prompts": doc.get("prompts", []),
        "description": doc.get("description", ""),
        "intro": doc.get("intro", ""),
        "title": doc.get("title", ""),
        "allow_file_upload": doc.get("allow_file_upload", False),
        "color": _normalize_color(doc.get("color")),
        "text_color": _normalize_text_color(doc.get("text_color")),
    }


@routes.get("/admin/modes")
@cognito_auth_required
def list_modes_admin():
    docs = []
    print("Listing modes for user:", request.user["sub"])
    query = {}
    if not request.user.get("is_super_admin"):
        query["user_id"] = request.user["sub"]
    for d in modes_collection.find(query):
        d["_id"] = str(d["_id"])
        d.pop("user_id", None)
        d["priority_source"] = _get_priority_source(d)
        d.pop("prioritize_files", None)
        d["color"] = _normalize_color(d.get("color"))
        d["text_color"] = _normalize_text_color(d.get("text_color"))
        docs.append(d)
    return {"modes": docs}


@routes.get("/admin/modes/<mode_id>")
@cognito_auth_required
def get_mode_admin(mode_id):
    query = {"_id": ObjectId(mode_id)}
    if not request.user.get("is_super_admin"):
        query["user_id"] = request.user["sub"]
    doc = modes_collection.find_one(query)
    if not doc:
        return {"error": "Not found"}, 404
    doc["_id"] = str(doc["_id"])
    doc["priority_source"] = _get_priority_source(doc)
    doc.pop("prioritize_files", None)
    doc.pop("user_id", None)
    doc["color"] = _normalize_color(doc.get("color"))
    doc["text_color"] = _normalize_text_color(doc.get("text_color"))
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
        "color": _normalize_color(data.get("color")),
        "text_color": _normalize_text_color(data.get("text_color")),
    }

    doc["priority_source"] = _get_priority_source(data)

    if "database" in data:
        doc["database"] = data.get("database")

    result = modes_collection.insert_one(doc)
    doc["_id"] = str(result.inserted_id)
    doc.pop("user_id", None)
    doc.pop("prioritize_files", None)
    return doc, 201


@routes.put("/admin/modes/<mode_id>")
@cognito_auth_required
def update_mode(mode_id):
    query = {"_id": ObjectId(mode_id)}
    if not request.user.get("is_super_admin"):
        query["user_id"] = request.user["sub"]
    doc = modes_collection.find_one(query)
    if not doc:
        return {"error": "Not found"}, 404
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
        "color": _normalize_color(data.get("color", doc.get("color", DEFAULT_MODE_COLOR))),
        "text_color": _normalize_text_color(data.get("text_color", doc.get("text_color", DEFAULT_TEXT_COLOR))),
    }

    update["priority_source"] = _get_priority_source(data)

    if "database" in data:
        update["database"] = data.get("database")
    
    modes_collection.update_one({"_id": doc["_id"]}, {"$set": update, "$unset": {"prioritize_files": ""}})
    doc.update(update)
    doc["_id"] = str(doc["_id"])
    doc.pop("user_id", None)
    doc.pop("prioritize_files", None)
    doc["color"] = _normalize_color(doc.get("color"))
    doc["text_color"] = _normalize_text_color(doc.get("text_color"))
    return doc


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
    cognito = boto3.client("cognito-idp", region_name=COGNITO_REGION)
    try:
        resp = cognito.initiate_auth(
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={"USERNAME": username, "PASSWORD": password},
            ClientId=COGNITO_APP_CLIENT_ID,
        )
    except cognito.exceptions.NotAuthorizedException:
        return {"error": "Invalid credentials"}, 401
    except Exception as e:  # noqa: BLE001
        print("cognito login failed", e)
        return {"error": "Login failed"}, 500
    auth = resp.get("AuthenticationResult", {})
    return {
        "id_token": auth.get("IdToken"),
        "access_token": auth.get("AccessToken"),
        "refresh_token": auth.get("RefreshToken"),
    }


@routes.get("/admin")
def admin_page():
    return send_from_directory(routes.static_folder, "admin.html")


@routes.get("/admin/analytics")
def admin_analytics_page():
    return send_from_directory(routes.static_folder, "admin_analytics.html")


@routes.get("/admin/mode")
def admin_mode_page():
    return send_from_directory(routes.static_folder, "mode_editor.html")


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

    pipeline = [{"$match": match}] if match else []

    def _isoformat_with_z(dt):
        if not dt:
            return None
        iso_value = dt.isoformat()
        if iso_value.endswith("Z") or "+" in iso_value[10:]:
            return iso_value
        return f"{iso_value}Z"

    total_prompts = prompt_logs_collection.count_documents({**match, "prompt": {"$exists": True}})
    total_responses = prompt_logs_collection.count_documents({**match, "response": {"$exists": True}})
    conversation_ids = [
        cid for cid in prompt_logs_collection.distinct("conversation_id", match) if cid
    ]
    unique_conversations = len(conversation_ids)
    ip_hashes = [
        ip for ip in prompt_logs_collection.distinct("ip_hash", match) if ip
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

    recent_prompts = []
    for doc in prompt_logs_collection.find(match).sort("created_at", -1).limit(20):
        created_at = doc.get("created_at")
        location = doc.get("location") or {}
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
        else:
            print("Mode ID is None")
        
        recent_prompts.append(
            {
                "prompt": doc.get("prompt", ""),
                "response": doc.get("response", ""),
                "mode": mode_title,
                "created_at": _isoformat_with_z(created_at),
                "location": {
                    "city": location.get("city"),
                    "region": location.get("region"),
                    "country": location.get("country"),
                },
            }
        )

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
        "recent_prompts": recent_prompts,
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

    pipeline = [{"$match": match}] if match else []

    try:
        # Use AI to interpret the query and generate appropriate analytics
        result = _process_natural_language_query(query, pipeline, match)
        return result
    except Exception as e:
        print(f"Error processing natural language query: {e}")
        return {"error": "Unable to process your question. Please try rephrasing it."}, 500


def _process_natural_language_query(query, pipeline, match):
    """Process natural language queries about analytics data using AI."""
    
    # Get current analytics data for context
    analytics_data = _get_analytics_data_for_query(pipeline, match)
    
    # Create a prompt for the AI to interpret the query
    system_prompt = """You are an analytics assistant that helps interpret natural language questions about user interaction data.

You have access to these tool functions:
- search_prompts(query_text): Search for prompts containing specific text or keywords
- get_top_prompts(): Get the most frequently used prompt messages and their counts

When answering questions about prompts:
- You can analyze specific prompts and how often they're used via the get_top_prompts tool function
- You can identify the most common questions or requests via the get_top_prompts tool function
- You can find patterns in user behavior based on prompt content via the get_top_prompts tool function
- You can answer questions about specific prompt text or topics via the search_prompts tool function
- You can search for prompts containing certain keywords or phrases via the search_prompts tool function

When answering questions:
1. Interpret what the user is asking for
2. Use the appropriate tool functions to gather specific data
3. Provide a clear, helpful answer based on the data
4. Include specific numbers and insights
5. If the question can't be answered confidently and in full with the available data, use the appropriate tool functions to gather specific data

Provide your response as a clear, conversational, human-readable answer. Include specific numbers, insights, and data points directly in your response. Be helpful and informative."""

    user_prompt = f"""Analytics Data:
{analytics_data}

User Question: "{query}"

Please analyze this question and provide insights based on the available data."""

    try:
        # Define the available tools for the AI (Responses API format)
        tools = [
            {
                "type": "function",
                "name": "search_prompts",
                "description": "Search for prompts containing specific text or keywords",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query_text": {
                            "type": "string",
                            "description": "The text or keywords to search for in prompts"
                        }
                    },
                    "required": ["query_text"]
                }
            },
            {
                "type": "function", 
                "name": "get_top_prompts",
                "description": "Get the most frequently used prompts",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of prompts to return (default: 20)"
                        }
                    },
                    "required": []
                }
            }
        ]
        
        # Use Responses API instead of Chat Completions
        response = client.responses.create(
            model="gpt-4o-mini",
            tools=tools,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        
        # Handle function calls if the AI wants to use tools
        if hasattr(response, 'output') and response.output and len(response.output) > 0:
            output = response.output[0]
            print("output", output)
            # Check for tool calls directly on the output
            if output.type == "function_call":

                print("tool call: ", output)
                function_name = output.name
                function_args = output.arguments
                
                # Parse function arguments
                if isinstance(function_args, str):
                    function_args = json.loads(function_args)
                
                # Execute the requested tool function
                if function_name == "search_prompts":
                    search_results = _search_prompts_tool(
                        function_args.get("query_text", ""), 
                        pipeline, 
                        match
                    )
                    tool_result = f"Found {len(search_results)} matching prompts:\n" + "\n".join([f"- \"{p['prompt']}\" (used {p['count']} times)" for p in search_results])
                    
                elif function_name == "get_top_prompts":
                    limit = function_args.get("limit", 20)
                    top_prompts = _get_unique_prompts_data(pipeline, match, limit)
                    tool_result = f"Top {len(top_prompts)} most frequent prompts:\n" + "\n".join([f"- \"{p['prompt']}\" (used {p['count']} times)" for p in top_prompts])
                else:
                    tool_result = "Unknown tool function requested."
                
                # Get the AI's final response with tool results
                final_response = client.responses.create(
                    model="gpt-4o-mini",
                    input=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                        output,
                        {"output": tool_result, "call_id": output.call_id, "type": "function_call_output"}
                    ]
                )
                
                # Use only the final response, not the original response
                ai_response = final_response.output_text.strip()
            else:
                print("no tool calls")
                # No tool calls, use direct response
                ai_response = response.output_text.strip()
        else:
            ai_response = response.output_text.strip()
        
        # Return the AI response directly as human-readable text
        return {
            "answer": ai_response
        }
            
    except Exception as e:
        print(f"Error calling AI for query processing: {e}")
        return {
            "error": "Unable to process your question with AI. Please try a simpler question."
        }


def _search_prompts_tool(query_text, pipeline, match, limit=20):
    """Tool function to search for prompts containing specific text or patterns."""
    
    # Create a regex pattern for case-insensitive search
    search_pattern = re.escape(query_text.lower())
    
    # Search for prompts containing the query text
    matching_prompts = []
    for doc in prompt_logs_collection.aggregate(
        pipeline + [
            {
                "$match": {
                    **match, 
                    "prompt": {
                        "$exists": True, 
                        "$ne": "",
                        "$regex": search_pattern,
                        "$options": "i"
                    }
                }
            },
            {"$group": {"_id": "$prompt", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit},
        ]
    ):
        prompt_text = doc.get("_id", "")
        count = doc.get("count", 0)
        
        # Truncate very long prompts for readability
        display_text = prompt_text[:200] + "..." if len(prompt_text) > 200 else prompt_text
        
        matching_prompts.append({
            "prompt": display_text,
            "full_prompt": prompt_text,
            "count": count
        })
    
    return matching_prompts


def _get_unique_prompts_data(pipeline, match, limit=50):
    """Get unique prompts and their repetition counts for AI analysis."""
    
    # Get unique prompts with their counts
    unique_prompts = []
    for doc in prompt_logs_collection.aggregate(
        pipeline + [
            {"$match": {**match, "prompt": {"$exists": True, "$ne": ""}}},
            {"$group": {"_id": "$prompt", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit},
        ]
    ):
        prompt_text = doc.get("_id", "")
        count = doc.get("count", 0)
        
        # Truncate very long prompts for readability
        display_text = prompt_text[:200] + "..." if len(prompt_text) > 200 else prompt_text
        
        unique_prompts.append({
            "prompt": display_text,
            "full_prompt": prompt_text,
            "count": count
        })
    print("unique_prompts", unique_prompts)
    return unique_prompts


def _get_analytics_data_for_query(pipeline, match):
    """Get relevant analytics data for AI processing."""
    
    # Get basic counts
    total_prompts = prompt_logs_collection.count_documents({**match, "prompt": {"$exists": True}})
    total_responses = prompt_logs_collection.count_documents({**match, "response": {"$exists": True}})
    unique_conversations = len([cid for cid in prompt_logs_collection.distinct("conversation_id", match) if cid])
    unique_users = len([ip for ip in prompt_logs_collection.distinct("ip_hash", match) if ip])
    
    # Get top modes
    mode_counts = [
        {"mode_id": doc.get("_id"), "count": doc.get("count", 0)}
        for doc in prompt_logs_collection.aggregate(
            pipeline + [
                {"$group": {"_id": "$mode", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 5},
            ]
        )
    ]
    
    top_modes_text = []
    for mode_data in mode_counts:
        mode_id = mode_data["mode_id"]
        if mode_id:
            try:
                mode_doc = modes_collection.find_one({"_id": ObjectId(mode_id)})
                mode_title = mode_doc.get("title") or mode_doc.get("name") if mode_doc else "Unknown"
            except Exception:
                mode_title = "Unknown"
        else:
            mode_title = "Unknown"
        top_modes_text.append(f"- {mode_title}: {mode_data['count']} interactions")
    
    # Get top countries
    top_countries = [
        {"country": doc.get("_id") or "Unknown", "count": doc.get("count", 0)}
        for doc in prompt_logs_collection.aggregate(
            pipeline + [
                {
                    "$group": {
                        "_id": {"$ifNull": ["$location.country", "Unknown"]},
                        "count": {"$sum": 1},
                    }
                },
                {"$sort": {"count": -1}},
                {"$limit": 5},
            ]
        )
    ]
    
    top_countries_text = [f"- {country['country']}: {country['count']} interactions" for country in top_countries]
    
    # Get daily activity (last 7 days)
    daily_counts = [
        {"date": doc.get("_id"), "count": doc.get("count", 0)}
        for doc in prompt_logs_collection.aggregate(
            pipeline + [
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
                {"$sort": {"_id": -1}},
                {"$limit": 7},
            ]
        )
    ]
    
    daily_text = [f"- {day['date']}: {day['count']} interactions" for day in daily_counts]
    
    return f"""Summary Statistics:
- Total Prompts: {total_prompts}
- Total Responses: {total_responses}
- Unique Conversations: {unique_conversations}
- Unique Users: {unique_users}

Top Modes by Usage:
{chr(10).join(top_modes_text)}

Top Countries by Usage:
{chr(10).join(top_countries_text)}

Recent Daily Activity (last 7 days):
{chr(10).join(daily_text)}"""


@routes.get("/admin/documents")
@cognito_auth_required
def list_documents_admin():
    mode = (request.args.get("mode") or "").strip()
    query = {}
    if not request.user.get("is_super_admin"):
        query["user_id"] = request.user["sub"]
    if mode:
        query["mode"] = mode
    docs = []
    for d in documents_collection.find(query):
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

    mode_query = {"name": mode} if mode else None
    if mode_query and not request.user.get("is_super_admin"):
        mode_query["user_id"] = request.user["sub"]
    mode_doc = modes_collection.find_one(mode_query) if mode_query else None
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
    return doc, 201


@routes.delete("/admin/documents/<doc_id>")
@cognito_auth_required
def delete_document(doc_id):
    query = {"_id": ObjectId(doc_id)}
    if not request.user.get("is_super_admin"):
        query["user_id"] = request.user["sub"]
    doc = documents_collection.find_one(query)
    if not doc:
        return {"error": "Not found"}, 404
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
    return {"status": "deleted"}



@routes.route("/")
def index():
    return send_from_directory(routes.static_folder, "index.html")

app = Flask(__name__, static_folder="public", static_url_path="")
if localDevMode == "true":
    app.register_blueprint(routes, url_prefix="/flask")
else:
    app.register_blueprint(routes)

if __name__ == "__main__":
    port = int(os.getenv("PORT", "3000"))
    app.run(port=port)