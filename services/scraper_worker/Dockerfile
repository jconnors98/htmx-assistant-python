# syntax=docker/dockerfile:1.7

ARG PYTHON_VERSION=3.10-slim
FROM python:${PYTHON_VERSION}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "services/scraper_worker/worker.py"]

