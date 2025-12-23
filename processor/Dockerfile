FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY processor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared /app/shared
COPY processor/src/ .

ENV PYTHONPATH=/app:/app/shared

CMD ["python", "main.py"]
