from decouple import config
import google.generativeai as genai


genai.configure(api_key=config("GEMINI_API_KEY"))


def ask_gemini(user_query: str) -> str:
    """Query the Gemini model with a prompt customised for TalentCentral."""
    prompt = (
        "You're a warm, helpful assistant supporting users on the TalentCentral platform.\n"
        "You help with construction jobs, training, apprenticeships, and workforce programs in British Columbia.\n"
        "Use a friendly, clear tone and provide links where appropriate using markdown.\n\n"
        f"User question: \"{user_query}\"\n"
    )
    model = genai.GenerativeModel("gemini-2.0-flash")
    chat = model.start_chat(history=[])
    result = chat.send_message(prompt)
    return result.text or ""