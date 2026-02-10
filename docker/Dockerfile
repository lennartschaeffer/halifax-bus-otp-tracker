FROM python:3.13-slim

WORKDIR /app

# Install uv for fast package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-dev

# Copy application code
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY main.py ./

# Create data directory
RUN mkdir -p /app/data/gtfs /app/data/archive

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Default command: run the poller
CMD ["uv", "run", "python", "main.py"]
