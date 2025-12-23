FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY utils /app/utils
COPY producer/src/ .

ENV PYTHONPATH=/app:/app/utils

CMD ["python", "main.py"]
