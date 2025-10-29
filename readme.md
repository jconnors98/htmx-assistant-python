# TalentCentral Assistant - HTMX + Python + OpenAI

This application can be embedded on other sites. Pass a `mode` query parameter
when loading the widget and the assistant will fetch prompts for that mode from
MongoDB.

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