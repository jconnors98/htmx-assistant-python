# Doc-Intel Worker Service

This service runs document-intelligence ingestion and package-build jobs outside
the Flask app. It polls the configured SQS queue, downloads source artifacts
from S3 or local storage, processes them, and writes metadata/results back to
MongoDB.

## Required Environment

At minimum, configure:

```bash
DOC_INTEL_SQS_QUEUE_URL=...
DOC_INTEL_SQS_REGION=us-east-1
DOC_INTEL_STORAGE_BACKEND=s3
DOC_INTEL_S3_BUCKET=...
DOC_INTEL_S3_PREFIX=doc_intel
OPENAI_API_KEY=...
MONGO_URI=...
MONGO_DB=...
```

If you use static AWS credentials locally, also set:

```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

## Local Run

```bash
python services/doc_intel_worker/worker.py
```

## Deployment Notes

- Designed for ECS/Fargate or EC2 auto-scaling workers.
- Install OCR/native dependencies in the container image:
  - `tesseract-ocr`
  - `libmagic1`
  - build essentials required by Python packages in `requirements.txt`
- Size tasks conservatively for OCR/PDF workloads and keep concurrency low.
