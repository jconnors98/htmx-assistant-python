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

print("Connected to MongoDB at", config("MONGO_URI"))

localDevMode = config("LOCAL_DEV_MODE", default="false").lower()



COGNITO_REGION = config("COGNITO_REGION", default=None)
COGNITO_USER_POOL_ID = config("COGNITO_USER_POOL_ID", default=None)
COGNITO_APP_CLIENT_ID = config("COGNITO_APP_CLIENT_ID", default=None)

S3_BUCKET = config("S3_BUCKET", default="builders-copilot")
s3 = boto3.client("s3")
_jwks = None


def _get_jwks():
    global _jwks
    if _jwks is None and COGNITO_REGION and COGNITO_USER_POOL_ID:
        url = (
            f"https://cognito-idp.{COGNITO_REGION}.amazonaws.com/"
            f"{COGNITO_USER_POOL_ID}/.well-known/jwks.json"
        )
        _jwks = requests.get(url, timeout=5).json().get("keys", [])
    return _jwks or []


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
            return {"error": "Unauthorized"}, 401
        try:
            claims = jwt.decode(
                token,
                key,
                algorithms=[headers.get("alg")],
                audience=COGNITO_APP_CLIENT_ID,
            )
        except Exception:
            return {"error": "Unauthorized"}, 401
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


@routes.post("/ask")
def ask():
    message = (request.form.get("message") or "").strip()
    mode = (request.form.get("mode") or "").strip()
    tag = (request.form.get("tag") or "").strip()
    if not message:
        return (
            '<div class="chat-entry assistant">'
            '<div class="bubble">⚠️ Message is required.</div>'
            "</div>"
        )

    try:
        mode_doc = modes_collection.find_one({"name": mode}) if mode else None
        mode_context = mode_doc.get("description", "") if mode_doc else ""
        mode_preferred_sites = mode_doc.get("preferred_sites", []) if mode_doc else []
        mode_blocked_sites = mode_doc.get("blocked_sites", []) if mode_doc else []
        allow_other_sites = mode_doc.get("allow_other_sites", True) if mode_doc else True
        prioritize_files = mode_doc.get("prioritize_files", False) if mode_doc else False
        tags = mode_doc.get("tags", []) if mode_doc else []
        vector_store_id = VECTOR_STORE_ID
        interest = mode if mode else "general BCCA information"
        tools = []

        gpt_system_prompt = (
            "You're a helpful, warm assistant supporting users with information about the BC Construction Association. "
            f"The user is interested in {interest}. {mode_context} "
        )

        if vector_store_id and mode:
            file_search_tool = {
                "type": "file_search",
                "vector_score_ids": [vector_store_id],
                "filters": {}
            }

            if tag and tag in tags:
                file_search_tool["filters"] = {
                    "type": "or",
                    "filters":[
                        {
                            "type": "and",
                            "filters":[
                                {
                                    "type": "eq",
                                    "property": "mode",
                                    "value": mode
                                },
                                {
                                    "type": "eq",
                                    "property": "tag",
                                    "value": tag
                                }
                            ]
                        },
                        {
                            "type": "and",
                            "filters":[
                                {
                                    "type": "eq",
                                    "property": "mode",
                                    "value": mode
                                },
                                {
                                    "type": "eq",
                                    "property": "always_include",
                                    "value": "true"
                                }
                            ]
                        }
                    ]
                }

            else:
                file_search_tool["filters"] = {
                    "type": "eq",
                    "property": "mode",
                    "value": mode
                }

            tools.append(file_search_tool)

            gpt_system_prompt += "You have access to a vector store containing relevant documents. "

            if prioritize_files:
                gpt_system_prompt += "When answering, prioritize information from the vector store first. Then search the following websites, listed in order of highest priority first, using the asterisk as a match-all character: "
            else:
                gpt_system_prompt += "When answering, use both the vector store and the following websites. They are listed in order of highest priority first, using the asterisk as a match-all character: "
        else:
            gpt_system_prompt += "When answering, use the following websites. They are listed in order of highest priority first, using the asterisk as a match-all character: "

        gpt_system_prompt += f"{', '.join(mode_preferred_sites)} "

        if allow_other_sites:
            gpt_system_prompt += (
                "If you cannot fully answer from these, then use other reputable sources. "
                f"Do not use the following sites as a source: {', '.join(mode_blocked_sites)} "
                "In your final answer, list internet sources from my preferred sites before listing any other internet sources."
            )
        else:
            gpt_system_prompt += (
                "Only use these websites in your web search; do not use any other sources from the internet."
            )

        tools.append({"type": "web_search_preview"})

        gpt_result = client.responses.create(
            model="gpt-4.1",
            tools=tools,
            input=[
                {
                    "role": "system",
                    "content": gpt_system_prompt,
                },
                {"role": "user", "content": message},
            ],
        )
        gpt_text = gpt_result.output_text
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
    }


@routes.get("/admin/modes")
@cognito_auth_required
def list_modes_admin():
    docs = []
    for d in modes_collection.find():
        d["_id"] = str(d["_id"])
        docs.append(d)
    return {"modes": docs}


@routes.post("/admin/modes")
@cognito_auth_required
def create_mode():
    data = request.get_json() or {}
    name = (data.get("name") or "").strip()
    if not name:
        return {"error": "Name is required"}, 400
    if modes_collection.find_one({"name": name}):
        return {"error": "Mode already exists"}, 400
    doc = {
        "name": name,
        "description": data.get("description", ""),
        "intro": data.get("intro", ""),
        "prompts": data.get("prompts", []),
        "preferred_sites": data.get("preferred_sites", []),
        "blocked_sites": data.get("blocked_sites", []),
        "allow_other_sites": data.get("allow_other_sites", True),
        "prioritize_files": data.get("prioritize_files", False),
    }
    result = modes_collection.insert_one(doc)
    doc["_id"] = str(result.inserted_id)
    return doc, 201


@routes.put("/admin/modes/<mode_id>")
@cognito_auth_required
def update_mode(mode_id):
    doc = modes_collection.find_one({"_id": ObjectId(mode_id)})
    if not doc:
        return {"error": "Not found"}, 404
    data = request.get_json() or {}
    update = {
        "name": data.get("name", doc.get("name")),
        "description": data.get("description", doc.get("description", "")),
        "intro": data.get("intro", doc.get("intro", "")),
        "prompts": data.get("prompts", doc.get("prompts", [])),
        "preferred_sites": data.get("preferred_sites", doc.get("preferred_sites", [])),
        "blocked_sites": data.get("blocked_sites", doc.get("blocked_sites", [])),
        "allow_other_sites": data.get("allow_other_sites", doc.get("allow_other_sites", True)),
        "prioritize_files": data.get("prioritize_files", doc.get("prioritize_files", False)),
    }
    modes_collection.update_one({"_id": doc["_id"]}, {"$set": update})
    doc.update(update)
    doc["_id"] = str(doc["_id"])
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


@routes.get("/admin/documents")
@cognito_auth_required
def list_documents_admin():
    docs = []
    for d in documents_collection.find({"user_id": request.user["sub"]}):
        d["_id"] = str(d["_id"])
        docs.append(d)
    return {"documents": docs}


@routes.post("/admin/documents")
@cognito_auth_required
def create_document():
    mode = (request.form.get("mode") or "").strip()
    content = request.form.get("content") or ""
    tags = [t.strip() for t in (request.form.get("tags") or "").split(",") if t.strip()]
    file = request.files.get("file")
    s3_keys = []
    openai_file_id = None
    if file:
        data = file.read()
        filename = file.filename
        for tag in tags or ["untagged"]:
            key = f"{mode}/{tag}/{filename}"
            s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data, ContentType=file.content_type)
            s3_keys.append(key)
        file_stream = io.BytesIO(data)
        openai_file = client.files.create(file=(filename, file_stream), purpose="assistants")
        openai_file_id = openai_file.id
        if VECTOR_STORE_ID:
            try:
                client.vector_stores.files.create(
                    vector_store_id=VECTOR_STORE_ID,
                    file_id=openai_file_id,
                    metadata={"mode": mode, "tags": tags},
                )
            except Exception as e:  # noqa: BLE001
                print("vector store add failed", e)
    doc = {
        "user_id": request.user["sub"],
        "mode": mode,
        "content": content,
        "tags": tags,
        "s3_keys": s3_keys,
        "openai_file_id": openai_file_id,
    }
    result = documents_collection.insert_one(doc)
    doc["_id"] = str(result.inserted_id)
    return doc, 201


@routes.put("/admin/documents/<doc_id>")
@cognito_auth_required
def update_document(doc_id):
    doc = documents_collection.find_one({"_id": ObjectId(doc_id), "user_id": request.user["sub"]})
    if not doc:
        return {"error": "Not found"}, 404
    content = request.form.get("content") or ""
    tags = [t.strip() for t in (request.form.get("tags") or "").split(",") if t.strip()]
    update = {"content": content, "tags": tags}
    documents_collection.update_one({"_id": doc["_id"]}, {"$set": update})
    doc.update(update)
    doc["_id"] = str(doc["_id"])
    return doc


@routes.delete("/admin/documents/<doc_id>")
@cognito_auth_required
def delete_document(doc_id):
    doc = documents_collection.find_one({"_id": ObjectId(doc_id), "user_id": request.user["sub"]})
    if not doc:
        return {"error": "Not found"}, 404
    for key in doc.get("s3_keys", []):
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