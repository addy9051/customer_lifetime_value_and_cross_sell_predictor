FROM python:3.11-slim

WORKDIR /app

# SECURITY: Create non-root user before installing anything (VULN-007)
RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install pip deps with retries and timeout for stability
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir --default-timeout=100 --retries 5 -r requirements.txt

# SECURITY: Copy only necessary source — not the entire project (VULN-009)
COPY api/ ./api/
COPY models/ ./models/
COPY features/ ./features/
COPY data/schema/ ./data/schema/
COPY marketing/ ./marketing/

# Set ownership and switch to non-root
RUN chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8000/health').raise_for_status()" || exit 1

# Run FastAPI
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
