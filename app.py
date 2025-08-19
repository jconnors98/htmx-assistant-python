import os
import re
from flask import Flask, request, send_from_directory
from markdown import markdown
import bleach
from decouple import config, Csv
from openai import OpenAI

# from gemini import ask_gemini

if not config("OPENAI_API_KEY") or not config("GEMINI_API_KEY"):
    raise RuntimeError("Missing API keys. Check your .env file.")

client = OpenAI(api_key=config("OPENAI_API_KEY"))

PRIORITY_SITES = config('PRIORITY_SITES', cast=Csv())
print("Using priority sites:", PRIORITY_SITES)

app = Flask(__name__, static_folder="public", static_url_path="")


@app.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
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
        mode_descriptions = {
            "Advocacy": "Focus on BCCA's advocacy efforts, policies, and industry priorities.",
            "Procurement": "Focus on procurement best practices and resources for BC construction.",
            "Workforce Solutions": "Focus on workforce solutions, jobs, training, and related programs.",
            "Membership": "Focus on BCCA membership benefits, joining, and governance information.",
        }
        mode_context = mode_descriptions.get(mode, "")
        interest = mode if mode else "general BCCA information"
        gpt_system_prompt = (
            "You're a helpful, warm assistant supporting users with information about the BC Construction Association. "
            f"The user is interested in {interest}. {mode_context} "
            "When answering, always try to search and use information from the following sites first, in this order of priority:"
            f"{', '.join(PRIORITY_SITES)}"
            "If you cannot fully answer from these, then use other reputable sources."
            "Do not use icba.ca as a source."
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
        # gemini_content = ask_gemini(message, PRIORITY_SITES)
        gpt_text = gpt_result.output_text
        # gemini_text = gemini_content or "🤖 Gemini had no response."

        # blended = client.responses.create(
        #     model="gpt-5",
        #     input=[
        #         {
        #             "role": "system",
        #             "content": (
        #                 "You're a writing assistant. Combine the two answers into a "
        #                 "clear, helpful, friendly response for users asking about "
        #                 "construction careers or training in BC. Do not repeat "
        #                 "points. Include links in markdown if available."
        #             ),
        #         },
        #         {
        #             "role": "user",
        #             "content": (
        #                 f"Blend these two answers:\n\n🔮 GPT says:\n{gpt_text}\n\n"
        #                 f"🌐 Gemini says:\n{gemini_text}"
        #             ),
        #         },
        #     ],
        # )

        # final_reply = blended.output_text
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


@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


if __name__ == "__main__":
    port = int(os.getenv("PORT", "3000"))
    app.run(port=port)