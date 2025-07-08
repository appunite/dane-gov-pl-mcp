# Use the full Python image to avoid missing system dependencies
FROM python:3.11-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    UV_NO_INDEX=1 \
    DEBIAN_FRONTEND=noninteractive

# Set the working directory inside the container
WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock ./
COPY src/ ./src/

EXPOSE 8000

# Install dependencies
RUN uv pip install --system --no-cache -r pyproject.toml

# Set the entry point
CMD ["uv", "run", "python", "-m", "src.app", "--transport", "streamable-http", "--host", "0.0.0.0", "--port", "8000"]

