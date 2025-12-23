FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY processor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY utils /app/utils
COPY processor/src/ .

ENV PYTHONPATH=/app:/app/utils

CMD ["python", "main.py"]
