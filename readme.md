# TalentCentral Assistant - HTMX + Python + OpenAI

This application can be embedded on other sites. Pass a `mode` query parameter
when loading the widget and the assistant will fetch prompts for that mode from
MongoDB.

## Embeddable Chat Widget

The application includes a modern, collapsible chat widget that can be embedded on any website or platform.

### Widget Features

- **Compact Start**: Begins as a single search bar, expands into full chat on first interaction
- **Fully Responsive**: Automatically adapts to mobile and desktop devices
- **File Upload Support**: Attach files when supported by the mode
- **Theme Customization**: Automatically matches your brand colors from mode configuration
- **Minimizable**: Users can collapse back to search bar at any time
- **Easy Integration**: Multiple embedding options (direct link, iframe, JavaScript)

### Quick Start

#### Option 1: JavaScript Widget Loader (Recommended)
```html
<script 
  src="https://your-domain.com/widget-loader.js" 
  data-mode="YOUR_MODE_ID" 
  data-theme="#82002d"
  data-position="bottom-right"
></script>
```

#### Option 2: IFrame Embed
```html
<iframe 
  src="https://your-domain.com/chat-widget.html?mode=YOUR_MODE_ID" 
  style="position: fixed; bottom: 20px; right: 20px; width: 420px; height: 600px; border: none; z-index: 9999;"
  allow="clipboard-write"
></iframe>
```

#### Option 3: Direct Link
```
https://your-domain.com/chat-widget.html?mode=YOUR_MODE_ID
```

### Widget Files

- **`public/chat-widget.html`** - Standalone embeddable chat widget
- **`public/widget-loader.js`** - JavaScript loader for easy embedding
- **`public/widget-demo.html`** - Complete documentation and examples
- **`public/index.html`** - Full-page assistant interface

### Configuration

The widget automatically pulls configuration from your mode settings:
- Primary and text colors
- Mode title and intro text
- File upload capabilities
- Custom prompts

For detailed embedding instructions and platform-specific examples, see `/widget-demo.html`.

## Environment

Set the following variables:

### Core Application
- `OPENAI_API_KEY` - OpenAI API key for GPT models
- `GEMINI_API_KEY` - Google Gemini API key
- `MONGO_URI` - MongoDB connection string
- `MONGO_DB` (optional, defaults to `assistant`) - MongoDB database name
- `OPENAI_VECTOR_STORE_ID` (optional) - OpenAI vector store ID for file search

### AWS Cognito Authentication
- `AWS_ACCESS_KEY_ID` - AWS access key
- `AWS_SECRET_ACCESS_KEY` - AWS secret key
- `COGNITO_REGION` - AWS region for Cognito (e.g., `us-east-1`)
- `COGNITO_USER_POOL_ID` - Cognito User Pool ID
- `COGNITO_APP_CLIENT_ID` - Cognito App Client ID

### Password Reset (AWS SES)
- `SES_SENDER_EMAIL` - Verified sender email address in Amazon SES

### Storage
- `S3_BUCKET` (optional, defaults to `builders-copilot`) - S3 bucket for file storage

### Development
- `LOCAL_DEV_MODE` (optional, defaults to `false`) - Set to `true` for local development

### Scraper Service
- `SCRAPER_EXECUTION_MODE` - `local` (default) keeps scraping inside the Flask app, `remote` pushes all jobs to the worker service.
- `SCRAPER_ENVIRONMENT` - Logical environment label stored with job documents (defaults to `prod`).
- `SCRAPER_SQS_QUEUE_URL` - Required when `SCRAPER_EXECUTION_MODE=remote`; points to the SQS queue consumed by the scraper worker.
- `SCRAPER_SQS_REGION` - AWS region for the scraper queue (defaults to `COGNITO_REGION` or `us-east-1`).
- `SCRAPER_SQS_MESSAGE_GROUP_ID` (optional) - FIFO message group for deterministic ordering.
- `SCRAPER_ENABLE_EMBEDDED_PDF_CHECKS` - Enable slow embedded-PDF detection (defaults to `false` in local dev, `true` elsewhere). Disable to speed up scrapes or avoid noisy shutdown logs.
- `SCRAPER_MAX_PDF_CHECKS_PER_PAGE` - Upper bound on viewer/embedded PDF checks per page (defaults to `50`).

The remote worker lives in `services/scraper_worker`. Run it locally with:

```bash
cd services/scraper_worker
pip install -r requirements.txt
python worker.py
```

For container deployments, build with:

```bash
docker build -f services/scraper_worker/Dockerfile . -t scraper-worker
```

Available modes and their prompts are stored in the `modes` collection.

## Password Reset Setup

The application uses AWS SES to send custom password reset emails with secure token-based links directly from the Flask application.

### Quick Setup

1. **Verify SES Email Address**
   - Go to AWS SES Console
   - Navigate to "Verified identities"
   - Create and verify your sender email address
   - For production use, request SES production access

2. **Set Environment Variable**
   - Add `SES_SENDER_EMAIL=your-verified-email@example.com` to your `.env` file

3. **IAM Permissions**
   - Ensure your AWS credentials have `ses:SendEmail` and `ses:SendRawEmail` permissions

For detailed setup instructions, see [SES_SETUP.md](./SES_SETUP.md).

### MongoDB Collections

- `modes` - Available assistant modes and prompts
- `documents` - Uploaded documents and knowledge base
- `prompt_logs` - Analytics and prompt history
- `superadmins` - Super admin user IDs
- `password_reset_tokens` - Password reset tokens (TTL indexed)
- `scrape_failures` - Per-page scrape failures with error metadata