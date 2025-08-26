# Dockerfile for Apache Beam streaming pipeline
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY pipeline/requirements.txt .

# Upgrade pip and install Python dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --force-reinstall -r requirements.txt

# Copy pipeline code and bigquery schema
COPY pipeline/ ./pipeline/
COPY bigquery/enriched_events_schema.json ./bigquery/enriched_events_schema.json

# Set Python path
ENV PYTHONPATH=/app

# Default command - configuration now comes from environment variables
CMD ["python3", "-m", "pipeline.streaming_new"]
