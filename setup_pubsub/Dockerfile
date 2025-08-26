# Dockerfile for Pub/Sub setup init container
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install Python dependencies directly
RUN pip install --no-cache-dir \
    google-cloud-pubsub==2.20.1 \
    requests==2.31.0

# Copy setup script
COPY setup_pubsub.py .

# Run setup script
CMD ["python3", "setup_pubsub.py"]
