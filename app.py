import os
import re
import io
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
from datetime import datetime
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

S3_BUCKET = config("S3_BUCKET", default="builders-copilot")
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=COGNITO_REGION
)
_jwks = None


def _get_jwks():
    global _jwks
    if _jwks is None and COGNITO_REGION and COGNITO_USER_POOL_ID:
        url = (
            f"https://cognito-idp.{COGNITO_REGION}.amazonaws.com/"
            f"{COGNITO_USER_POOL_ID}/.well-known/jwks.json"
        )
        _jwks = requests.get(url, timeout=30).json().get("keys", [])
    return _jwks or []


def cognito_auth_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        print("Authorization header:", auth)
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
        request.user = {"sub": claims.get("sub")}
        return fn(*args, **kwargs)

    return wrapper


routes = Blueprint("routes", __name__, static_folder='public', static_url_path='')

@routes.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Content-Security-Policy"] = "frame-ancestors *"
    return response

def _async_log_prompt(prompt, mode, ip_addr, conversation_id):
    ip_hash = hashlib.sha256(ip_addr.encode()).hexdigest() if ip_addr else None
    location = {}
    print(f"Logging prompt from IP: {ip_addr}")
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

    prompt_logs_collection.insert_one({
            "prompt": prompt,
            "mode": mode,
            "conversation_id": conversation_id,
            "ip_hash": ip_hash,
            "location": location,
            "created_at": datetime.utcnow(),
        })


@routes.post("/ask")
def ask():
    message = (request.form.get("message") or "").strip()
    mode = (request.form.get("mode") or "").strip()
    tag = (request.form.get("tag") or "").strip()
    conversation_id = (request.form.get("conversation_id") or "").strip()
    previous_response_id = (request.form.get("response_id") or "").strip()
    file = request.files.get("file")
    openai_file_id = None
    if file and file.filename:
        data = file.read()
        file_stream = io.BytesIO(data)
        uploaded = client.files.create(
            file=(file.filename, file_stream), purpose="assistants"
        )
        openai_file_id = uploaded.id

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
    threading.Thread(target=_async_log_prompt, args=(message, mode, ip_addr, conversation_id)).start()

    try:
        user_id = getattr(request, "user", {}).get("sub", "anonymous")
        gpt_text, response_id, _usage = conversation_service.respond(
            conversation_id=conversation_id,
            user_id=user_id,
            text=message,
            mode=mode,
            tag=tag,
            previous_response_id=previous_response_id or None,
            file_id=openai_file_id,
        )

        if openai_file_id:
            try:
                client.files.delete(openai_file_id)
            except Exception as e:  # noqa: BLE001
                print("openai file delete failed", e)


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
            '<div class="source-tag">Powered by BCCA</div>'
            "</div></div>"
            f'<input type="hidden" id="conversation_id" name="conversation_id" value="{conversation_id}" hx-swap-oob="true"/>'
            f'<input type="hidden" id="response_id" name="response_id" value="{response_id}" hx-swap-oob="true"/>'
        )

        return html
    except Exception as err:  # noqa: BLE001
        print("❌ Error blending AI responses:", err)
        return (
            '<div class="chat-entry assistant">'
            '<div class="bubble">❌ There was an error getting a response. Please try again.</div>'
            "</div>"
        )


@routes.get("/modes")
def list_modes():
    docs = list(modes_collection.find({}, {"_id": 0}))
    return {"modes": docs}


@routes.get("/modes/<mode>")
def get_mode(mode):
    doc = modes_collection.find_one({"name": mode}, {"_id": 0})
    if not doc:
        return {"prompts": []}, 404
    return {
        "prompts": doc.get("prompts", []),
        "description": doc.get("description", ""),
        "intro": doc.get("intro", ""),
        "title": doc.get("title", ""),
    }


@routes.get("/admin/modes")
@cognito_auth_required
def list_modes_admin():
    docs = []
    print("Listing modes for user:", request.user["sub"])
    for d in modes_collection.find({"user_id": request.user["sub"]}):
        d["_id"] = str(d["_id"])
        d.pop("user_id", None)
        docs.append(d)
    return {"modes": docs}


@routes.get("/admin/modes/<mode_id>")
@cognito_auth_required
def get_mode_admin(mode_id):
    doc = modes_collection.find_one({"_id": ObjectId(mode_id), "user_id": request.user["sub"]})
    if not doc:
        return {"error": "Not found"}, 404
    doc["_id"] = str(doc["_id"])
    doc.pop("user_id", None)
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
        "prioritize_files": data.get("prioritize_files", False),
    }
    result = modes_collection.insert_one(doc)
    doc["_id"] = str(result.inserted_id)
    doc.pop("user_id", None)
    return doc, 201


@routes.put("/admin/modes/<mode_id>")
@cognito_auth_required
def update_mode(mode_id):
    doc = modes_collection.find_one({"_id": ObjectId(mode_id), "user_id": request.user["sub"]})
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
        "prioritize_files": data.get("prioritize_files", doc.get("prioritize_files", False)),
    }
    modes_collection.update_one({"_id": doc["_id"]}, {"$set": update})
    doc.update(update)
    doc["_id"] = str(doc["_id"])
    doc.pop("user_id", None)
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


@routes.get("/admin/mode")
def admin_mode_page():
    return send_from_directory(routes.static_folder, "mode_editor.html")


@routes.get("/admin/documents")
@cognito_auth_required
def list_documents_admin():
    mode = (request.args.get("mode") or "").strip()
    query = {"user_id": request.user["sub"]}
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

    mode_doc = modes_collection.find_one({"name": mode, "user_id": request.user["sub"]}) if mode else None
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
        "user_id": request.user["sub"],
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
    doc = documents_collection.find_one({"_id": ObjectId(doc_id), "user_id": request.user["sub"]})
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