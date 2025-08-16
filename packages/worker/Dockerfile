FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

# Get dependencies
COPY pyproject.toml .
RUN uv sync --no-cache --no-dev

COPY src/ .

CMD ["uv", "run", "--no-sync", "--no-cache", "main.py"]
