# TalentCentral Assistant - HTMX + Python + OpenAI

This application can be embedded on other sites. Pass a `mode` query parameter
when loading the widget and the assistant will fetch prompts for that mode from
MongoDB.

## Environment

Set the following variables:

- `OPENAI_API_KEY`
- `GEMINI_API_KEY`
- `MONGO_URI`
- `MONGO_DB` (optional, defaults to `assistant`)

Available modes and their prompts are stored in the `modes` collection.