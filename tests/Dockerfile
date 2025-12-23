FROM python:3.11-slim

WORKDIR /app

# Copy requirements files
COPY requirements-dev.txt .
COPY processor/requirements.txt ./processor-requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r requirements-dev.txt
RUN pip install --no-cache-dir -r processor-requirements.txt

# Copy source code
COPY utils/ ./utils/
COPY processor/ ./processor/
COPY producer/ ./producer/
COPY tests/ ./tests/

# Set Python path
ENV PYTHONPATH=/app

# Run tests
CMD ["python", "-m", "unittest", "discover", "-s", "tests", "-v"]

