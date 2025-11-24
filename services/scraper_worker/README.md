# Scraper Worker Service

This package houses the auto-scaling scraper worker that runs outside the Flask
app. It continuously consumes jobs from the configured SQS queue, executes
them with `ScrapeJobProcessor`, and writes results back to MongoDB.

## Features

- Modular queue consumer powered by `packages.common.scraper_contracts`
- Shares business logic via `scraper_jobs.ScrapeJobProcessor`
- Supports `scrape`, `single_url_refresh`, `delete_content`, and `verification`
  job types
- Designed for ECS/Fargate or EC2 Auto Scaling deployments

## Local Development

1. Copy `.env.example` (coming from the root project) and set the following:
   ```
   SCRAPER_SQS_QUEUE_URL=...
   SCRAPER_SQS_REGION=us-east-1
   OPENAI_API_KEY=...
   MONGO_URI=...
   ```
2. Install dependencies:
   ```bash
   cd services/scraper_worker
   pip install -r requirements.txt
   ```
3. Run the worker:
   ```bash
   python worker.py
   ```

## Deployment

The companion `Dockerfile` builds a slim container suitable for ECS Fargate or
an EC2 auto-scaling group. Provide AWS credentials through the task role or
instance profile so the worker can poll SQS and access S3/Secrets Manager.

