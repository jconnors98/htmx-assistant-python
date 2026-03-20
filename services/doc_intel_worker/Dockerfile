# syntax=docker/dockerfile:1.7

ARG PYTHON_VERSION=3.14.0
FROM python:${PYTHON_VERSION}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    libmagic1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY assistant_services ./assistant_services
COPY document_intelligence_jobs.py ./document_intelligence_jobs.py
COPY document_intelligence_service.py ./document_intelligence_service.py
COPY document_intelligence_storage.py ./document_intelligence_storage.py
COPY models ./models
COPY packages ./packages
COPY services/doc_intel_worker ./services/doc_intel_worker
COPY tools ./tools

CMD ["python", "services/doc_intel_worker/worker.py"]
