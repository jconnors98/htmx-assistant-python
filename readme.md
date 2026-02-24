# htmx-assistant-python

Python/Flask backend that powers a multi-mode HTMX chat assistant, ties OpenAI’s Assistants + vector stores to user-uploaded knowledge, and orchestrates a Playwright-driven scraping pipeline with optional remote workers.

## Overview

The application serves two audiences:

- **End users** interact with a chat UI or embeddable widget that talks to OpenAI Responses/Assistants. Each “mode” can point the assistant at curated prompt templates, uploaded files, or pre-scraped websites.
- **Administrators** create/manage modes, upload documents, trigger scrapes, inspect analytics, and manage credentials via Cognito-protected endpoints.

Behind the UI sits a Mongo-backed conversation log, S3-backed document store, OpenAI vector search, and an APScheduler/SQS powered scraping fleet that discovers and maintains web content for each mode.

## Key Capabilities

- Single conversation endpoint (`/ask`) that supports per-mode prompts, tags, and OpenAI file attachments.
- Mode-aware system prompts, live web search, and vector store grounding via `ConversationService`.
- Admin APIs for authentication, analytics, document management, password reset (SES), and Cognito token refresh.
- Scraping pipeline built around Playwright with automatic sitemap crawling, downloadable file discovery, verification passes, and job persistence in MongoDB.
- Queue-friendly scraper client that can run locally or push work to AWS SQS and a remote worker pool (`services/scraper_worker`).
- Compliance-friendly audit trails: prompt/response logs with IP hashing, scrape job records, and scrape verification stats.

## Architecture Overview

```
Browser/UI ──▶ Flask app (`app.py`)
                │
                ├─ ConversationService → OpenAI Responses + Vector Store
                ├─ MongoDB (conversations, modes, jobs, scraped content)
                ├─ AWS Cognito (auth) / SES (email) / S3 (document blobs)
                ├─ ScrapeScheduler → ScraperClient
                │      ├─ local mode → ScrapingService (Playwright)
                │      └─ remote mode → SQS → scraper_worker → ScrapingService
                └─ MySQL (permits search mode)
```

## Core Components

### Flask application (`app.py`)

- Boots the entire service, wires configuration via `python-decouple`, and instantiates MongoDB, OpenAI, Cognito, SES, and S3 clients.
- Exposes HTMX-friendly routes for chat (`/ask`), file upload, and static assets from `public/`.
- Provides extensive admin APIs (`/admin/**`) for mode CRUD, analytics, document ingestion, scraping controls, password reset flows, and Cognito token refresh.
- Starts `ScrapeScheduler` on process boot so recurring scrapes and verification continue while the Flask process runs.

### Conversation engine (`conversation_service.py`)

- Persists per-conversation transcripts, summaries, and file attachments in MongoDB collections (`conversations`, `messages`, `summaries`).
- Dynamically crafts system prompts + tool definitions per mode, enabling vector store file search, live web search, or permits search (MySQL) when needed.
- Manages uploaded file lifecycles (store, reuse, cleanup) and enforces max message windows per conversation.

### Scraping subsystem

- `scraping_service.py` runs site crawls with Playwright + BeautifulSoup, caches browsers in a pooling layer, discovers downloadable files, stores scraped HTML/text, and pushes content into OpenAI vector stores.
- `scraper_jobs.py` centralizes job execution (scrape, verification, delete, refresh, site delete) and ensures Mongo job docs stay in sync.
- `assistant_services/ScraperClient` abstracts dispatching jobs either locally (threads) or remotely (SQS) while recording progress in `scraping_jobs`.
- `scrape_scheduler.py` (APScheduler) queues daily/weekly scrapes, triggers verification batches, resumes orphaned jobs, and enforces concurrent job limits.
- `services/scraper_worker` hosts an SQS consumer that runs the same job processor out-of-process; the accompanying `Dockerfile` builds a slim worker image with Chromium Playwright dependencies.

### Admin, analytics, and utilities

- `functions.py` houses shared helpers (JWKS fetch/cache, prompt logging, analytics aggregation, permit search tool hooks, color normalization, etc.).
- Analytics endpoints expose aggregated prompt counts, geographic summaries, top modes, and natural-language analytics queries that get routed back through `_process_natural_language_query`.
- Password reset flows generate SES emails with branded HTML, store tokens in MongoDB, and reset Cognito passwords via admin APIs.

### Frontend surfaces (`public/`)

- Contains HTMX pages for the chat UI (`index.html`), admin dashboard (`admin*.html`), mode editor, analytics dashboards, and embeddable widget assets (`widget-loader.js`, `chat-widget.html`).
- When `LOCAL_DEV_MODE=true`, the Flask blueprint is mounted under `/flask` so static files can be served side-by-side with an upstream reverse proxy.

### Documentation & future specs

- `documentation/zip_extractor_feature.md` documents the planned “Construction Document Intelligence Mode” along with backend tool contracts (zip extractor, OCR, structured extraction, etc.). Use this as the canonical build spec when extending the assistant with document-intelligence tooling.

## External services & data stores

| Service | Purpose | Key configuration |
| --- | --- | --- |
| MongoDB Atlas or self-hosted | Conversations, prompts, modes, scrape metadata, job queue, reset tokens | `MONGO_URI`, `MONGO_DB` |
| OpenAI Responses + Vector Store | Chat completions and file-grounded search | `OPENAI_API_KEY`, `OPENAI_VECTOR_STORE_ID` |
| AWS Cognito | Admin authentication (ID/access/refresh tokens) | `COGNITO_REGION`, `COGNITO_USER_POOL_ID`, `COGNITO_APP_CLIENT_ID` |
| AWS SES | Password reset email delivery | `SES_SENDER_EMAIL`, AWS credentials |
| AWS S3 | Persistent storage for uploaded files before vector ingestion | `S3_BUCKET`, AWS credentials |
| AWS SQS | Remote scraper job queue (optional) | `SCRAPER_SQS_QUEUE_URL`, `SCRAPER_SQS_REGION`, `SCRAPER_SQS_MESSAGE_GROUP_ID` |
| Playwright (Chromium) | Dynamic website rendering & scraping | Installed via `playwright install chromium` |
| MySQL | Permits search tool used by `permitsca` mode | `MYSQL_HOST`, `MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_CERT_PATH` |

## Repository layout (trimmed)

```
.
├── app.py                         # Flask entrypoint + routes
├── conversation_service.py        # Chat orchestration + tool wiring
├── scraping_service.py            # Playwright crawler & ingestion logic
├── scrape_scheduler.py            # APScheduler wrapper
├── scraper_jobs.py                # Shared job processor
├── assistant_services/
│   └── scraper_client.py          # Local vs remote dispatch layer
├── services/scraper_worker/       # SQS worker + Dockerfile
├── public/                        # HTMX/chat/admin front-end assets
├── documentation/zip_extractor_feature.md
├── functions.py                   # Shared helpers/utilities
├── requirements.txt               # Flask app deps
├── Dockerfile                     # Worker container image
└── playwriter_env_check.py        # Standalone Playwright diagnostics
```

*(See `services/scraper_worker/README.md` for worker-specific docs.)*

## Getting started

### Prerequisites

- Python 3.10+ and `pip`
- MongoDB instance reachable from the app
- OpenAI API key with Responses + Vector Store access
- AWS account (S3, SES, Cognito, SQS) if you plan to use the hosted integrations
- Playwright system dependencies (`playwright install chromium && playwright install-deps chromium` on Linux)
- MySQL database if you intend to use the permits search mode

### Environment variables

Create a `.env` (or export variables) with at least:

| Variable | Description |
| --- | --- |
| `OPENAI_API_KEY`, `OPENAI_VECTOR_STORE_ID` | OpenAI credentials and optional vector store id |
| `MONGO_URI`, `MONGO_DB` | Mongo connection string + database name |
| `LOCAL_DEV_MODE` | Set to `true` to mount routes under `/flask` and skip Playwright path overrides |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Required for S3/SES/SQS when not using instance roles |
| `S3_BUCKET` | Bucket storing uploaded documents |
| `SES_SENDER_EMAIL` | Verified SES sender for password reset emails |
| `COGNITO_REGION`, `COGNITO_USER_POOL_ID`, `COGNITO_APP_CLIENT_ID` | Cognito auth metadata |
| `TALENTCENTRAL_USER_TOKEN_SECRET` | Shared HS256 secret used to verify TalentCentral widget user JWTs (`uid`, `role`, `iat`, `exp`) |
| `SCRAPER_EXECUTION_MODE` | `local` (default) or `remote`; controls whether scraping runs in-process or via SQS |
| `SCRAPER_SQS_QUEUE_URL`, `SCRAPER_SQS_REGION`, `SCRAPER_SQS_MESSAGE_GROUP_ID` | Required when `SCRAPER_EXECUTION_MODE=remote` |
| `SCRAPER_BROWSER_POOL_SIZE`, `SCRAPER_MAX_CONCURRENT_JOBS` | Tunables for crawler concurrency |
| `MYSQL_*` vars | Host/database/user/password/cert for permits search |

Refer to `scraping_service.py` and `app.py` for additional env knobs (crawler delays, verification batch sizes, etc.).

### Install dependencies

```bash
python -m venv .venv
. .venv/Scripts/activate  # or source .venv/bin/activate on Unix
pip install -r requirements.txt
playwright install chromium  # once per machine
```

### Running the Flask app

```bash
export LOCAL_DEV_MODE=true  # optional
python app.py
```

- In local mode, the app serves routes under `/flask` to avoid clashing with upstream proxies.
- The scrape scheduler starts automatically; stop the process cleanly so it can call `scrape_scheduler.stop()`.

### Scraper execution modes

- **Local (`SCRAPER_EXECUTION_MODE=local`)** – Scraping jobs run inside the Flask process using worker threads. This is best for development or low-volume single-node deployments.
- **Remote (`SCRAPER_EXECUTION_MODE=remote`)** – Flask enqueues jobs into SQS and a separate worker fleet consumes them. You must set `SCRAPER_SQS_QUEUE_URL`, `SCRAPER_SQS_REGION`, and (for FIFO queues) `SCRAPER_SQS_MESSAGE_GROUP_ID`.

### Running the scraper worker

You can run the worker directly:

```bash
cd services/scraper_worker
python worker.py
```

Or build the provided container (includes Chromium dependencies) and deploy to ECS/Fargate/EC2:

```bash
docker build -t scraper-worker -f Dockerfile .
docker run --env-file ../.env scraper-worker
```

### Playwright environment check

Use `python playwright_env_check.py --url https://example.com` to confirm Chromium binaries, scraping dependencies, and rendering flags before running the full service. The script boots a stub `ScrapingService`, performs a scrape, and reports title/content sizes.

## Scraping workflow

1. **Mode configuration** – Admins define `scrape_sites`, frequency (manual/daily/weekly), preferred/blocked hosts, and color/tag metadata via `/admin/modes`.
2. **Scheduling/queueing** – `ScrapeScheduler` (or manual triggers via `/admin/scrape/trigger/<mode_id>`) queues jobs in `scraping_jobs`.
3. **Dispatch** – `ScraperClient` either spins up local threads or packages SQS messages that remote workers consume.
4. **Crawl** – `ScrapingService` walks sitemap + in-page links, handles dynamic pages via Playwright, extracts text + HTML, detects downloadable files, uploads text to the OpenAI vector store, and writes metadata to Mongo (`scraped_content`, `scraped_sites`, `discovered_files`).
5. **Verification & refresh** – Periodic verification jobs re-scrape pages with a heavier “merge dynamic” pass, compare content, and update vector entries when differences exceed thresholds.
6. **Admin actions** – Admin endpoints expose scrape status, discovered files (with add/block/delete actions), per-site summaries, and job history.

## Conversation workflow

1. **Request ingress** – `/ask` collects the user prompt, active mode, tag, and uploaded OpenAI file ids (managed via `/upload-files` or admin document uploads).
2. **Logging** – Prompts/responses stream into `prompt_logs` asynchronously along with IP country/city metadata for analytics.
3. **Context building** – `ConversationService` composes summaries + truncated history, resolves mode instructions, decides which tools (vector file search, web search, permits search) to expose, and attaches any uploaded files for the session.
4. **Model call** – Calls `gpt-5-mini` via Responses API; if confidence is low the service automatically escalates to `gpt-5.1`.
5. **File/vector integration** – When modes have `has_files` or scraped content, the service injects vector store filters so the model can ground its answers before resorting to web searches.
6. **Post-processing** – Responses are rendered through `markdown`, sanitized via `bleach`, and wrapped in HTMX-compatible HTML for the chat UI.

## API highlights

- **Public chat**: `/upload-files`, `/ask`, `/clear-conversation`, `/modes`, `/modes/<name>`.
- **Admin auth**: `/admin/login`, `/admin/reset/*`, `/api/refresh-token`, `/admin/user`.
- **Mode management**: CRUD under `/admin/modes`, including color/tag, prompt templates, and priority sources.
- **Document ingestion**: `/admin/documents` (list/create/delete/download) plus `/admin/scrape/add-file` and related discovered-file endpoints.
- **Scraping controls**: `/admin/scrape/trigger`, `/admin/scrape/status`, `/admin/scrape/jobs`, `/admin/scrape/discovered-files`, `/admin/scrape/site/*`, `/admin/scraped-content`.
- **Analytics**: `/admin/analytics/*` endpoints deliver summaries, natural-language analytics search, and conversation transcript exports.
- **Permits API**: `/api/permitsca` is a JSON-only variant of `/ask` that forces the `permitsca` mode and exposes usage metrics.
- **TalentCentral API**: `/api/talentcentral` accepts a JSON `prompt` and `user_id`, forces `talentcentral` mode, and passes `user_id` into the jobs tool flow.

### TalentCentral API example

Use an API token header (`Authorization: Bearer ...`, `X-API-Token`, or `X-API-Key`) because this route uses `token_auth_required`.

```bash
curl -X POST "http://localhost:3000/api/talentcentral" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: YOUR_API_TOKEN" \
  -d '{
    "prompt": "Find apprentice electrician jobs near me",
    "user_id": "123",
    "conversation_id": "optional-conversation-id",
    "response_id": "optional-previous-response-id",
    "tag": "optional-tag"
  }'
```

Example response:

```json
{
  "response": "Here are some relevant roles...",
  "conversation_id": "67bd4c6f2e5f0b2c8450a111",
  "response_id": "resp_abc123",
  "usage": 1853,
  "mode": "talentcentral",
  "tag": "",
  "user_id": "123"
}
```

### TalentCentral widget user token (`/ask`)

For embedded chat widget usage in `talentcentral` mode, you can pass a short-lived user JWT from your host page:

```html
<script
  src="https://bcca.ai/flask/widget-loader.js"
  data-mode="talentcentral"
  data-user-token="{{user-token}}">
</script>
```

Token expectations:
- JWT signed with HS256 using `TALENTCENTRAL_USER_TOKEN_SECRET` on the chatbot backend.
- Claims should include `uid`, `role`, `iat`, and `exp`.
- The widget forwards this token on `/ask` only for `talentcentral` mode.
- If token is missing, invalid, or expired, the request is treated as guest (`anonymous`).

All admin routes are wrapped in `cognito_auth_required`, ensuring Bearer tokens from Cognito are verified against JWKS metadata and user roles (super admin vs regular) are enforced.

## Background jobs & monitoring

- Job state is persisted in `scraping_jobs` with fields for progress, checkpoints, environment labels, and timestamps (`created_at`, `started_at`, `completed_at`).
- Verification statistics are available through `ScraperClient.get_verification_statistics()` and surfaced in admin endpoints.
- Prompt logs capture hashed IPs and geodata for rate tracking. When exploring analytics, the `_process_natural_language_query` helper lets admins ask free-form questions that the AI answers after running aggregation pipelines.

## Document intelligence roadmap

The repository already includes the detailed build spec for the “Construction Document Intelligence Mode” in `documentation/zip_extractor_feature.md`. Implementations should follow that contract by adding tool modules (`tools/extract.py`, `tools/ocr.py`, etc.), new dataclasses (`DocumentMetadata`, `ProjectContext`, `BidPackage`), and workflow glue so the assistant can ingest drawings, run OCR, classify trades, and assemble bid packages. Treat the README you’re reading now as the description of today’s state; treat the spec as the blueprint for the next major feature set.

## Deployment automation

This repo includes a baseline SSH-based GitHub Actions deployment pipeline:

- GitHub workflow: `.github/workflows/deploy.yml`
- Remote deploy target path: `/opt/projects/htmx-assistant-python`
- Remote restart command: `sudo systemctl restart apache2`

Setup instructions and required AWS/GitHub configuration live in `docs/AWS_CODEDEPLOY_GITHUB_ACTIONS.md`.

## Contributing & next steps

- Add unit/integration tests around `ConversationService`, scraping utilities, and admin flows as new functionality lands (current repo is test-light).
- Wire the planned document intelligence tools into the existing mode system so new capabilities feel native to each mode.
- Expand the admin UI in `public/` to surface scrape verification stats, discovered file triage, and document intelligence workflows.
- Keep `requirements.txt` and Playwright versions aligned across the Flask app and worker to avoid mismatched Chromium installs.

Happy hacking!

