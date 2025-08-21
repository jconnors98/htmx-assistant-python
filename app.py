import os
import re
from flask import Flask, request, send_from_directory
from markdown import markdown
import bleach
from decouple import config
from openai import OpenAI
from pymongo import MongoClient
from pymongo.server_api import ServerApi


if not config("OPENAI_API_KEY"):
    raise RuntimeError("Missing API key. Check your .env file.")

client = OpenAI(api_key=config("OPENAI_API_KEY"))

mongo_client = MongoClient(config("MONGO_URI"), server_api=ServerApi("1"))
try:
    mongo_client.admin.command("ping")
    print("MongoDB connection successful.")

except Exception as e:
    raise RuntimeError(f"Failed to connect to MongoDB: {e}")

db = mongo_client.get_database(config("MONGO_DB", default="bcca-assistant"))
modes_collection = db.get_collection("modes")

print("Connected to MongoDB at", config("MONGO_URI"))
app = Flask(__name__, static_folder="public", static_url_path="")


@app.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Content-Security-Policy"] = "frame-ancestors *"
    return response


@app.post("/ask")
def ask():
    message = (request.form.get("message") or "").strip()
    mode = (request.form.get("mode") or "").strip()
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
        interest = mode if mode else "general BCCA information"
        gpt_system_prompt = (
            "You're a helpful, warm assistant supporting users with information about the BC Construction Association. "
            f"The user is interested in {interest}. {mode_context} "
            "When answering, always try to search and use information from the following sites first, in this order of priority, using the asterisk as a match-all character:"
            f"{', '.join(mode_preferred_sites)}"
            "If you cannot fully answer from these, then use other reputable sources."
            f"Do not use the following sites as a source: {', '.join(mode_blocked_sites)}"
            "In your final answer, list sources from my preferred sites separately before listing any other sources."
        )
        gpt_result = client.responses.create(
            model="gpt-4.1",
            tools=[{ "type": "web_search_preview" }],
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
            tags=list(bleach.sanitizer.ALLOWED_TAGS) + ["img", "p", "h3"],
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


@app.get("/modes")
def list_modes():
    docs = list(modes_collection.find({}, {"_id": 0}))
    return {"modes": docs}


@app.get("/modes/<mode>")
def get_mode(mode):
    doc = modes_collection.find_one({"name": mode}, {"_id": 0})
    if not doc:
        return {"prompts": []}, 404
    return {
        "prompts": doc.get("prompts", []),
        "description": doc.get("description", ""),
        "intro": doc.get("intro", ""),
    }


@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


if __name__ == "__main__":
    port = int(os.getenv("PORT", "3000"))
    app.run(port=port)