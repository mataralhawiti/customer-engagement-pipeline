# Dockerfile for Apache Beam streaming pipeline
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies in one layer
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --upgrade pip

# Copy requirements first for better caching
COPY pipeline/requirements.txt .

# Install Python dependencies with optimizations
RUN pip install --no-cache-dir \
    --disable-pip-version-check \
    --no-compile \
    -r requirements.txt

# Copy pipeline code and bigquery schema
COPY pipeline/ ./pipeline/
COPY bigquery/enriched_events_schema.json ./bigquery/enriched_events_schema.json

# Set Python path
ENV PYTHONPATH=/app

# Default command - configuration now comes from environment variables
CMD ["python3", "-m", "pipeline.streaming_new"]
