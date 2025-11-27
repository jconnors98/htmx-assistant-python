# syntax=docker/dockerfile:1.7

ARG PYTHON_VERSION=3.10-slim
FROM python:${PYTHON_VERSION}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only the modules required by the scraper worker
COPY scraping_service.py ./scraping_service.py
COPY scraper_jobs.py ./scraper_jobs.py
COPY packages ./packages
COPY services/scraper_worker ./services/scraper_worker

RUN playwright install chromium
RUN playwright install-deps chromium

CMD ["python", "services/scraper_worker/worker.py"]
