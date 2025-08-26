# Use specific version for reproducibility and security
FROM python:3.11.9-slim-bookworm

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Create non-root user
RUN groupadd -r etl && useradd -r -g etl -d /app -s /sbin/nologin etl

WORKDIR /app

# Update base packages and remove apt cache
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY --chown=etl:etl requirements.txt .

# Install Python dependencies as root, then change ownership
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY --chown=etl:etl . .

# Create output directory with correct permissions
RUN mkdir -p /app/output && chown -R etl:etl /app/output

# Switch to non-root user
USER etl

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Run pipeline
CMD ["python", "-m", "src.pipeline"]