FROM python:3.13-slim AS base

RUN useradd -m appuser
WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY packages ./packages
COPY services ./services

RUN pip install --no-cache-dir uv \
  && uv sync --frozen --no-dev --all-packages

ENV PATH="/app/.venv/bin:$PATH"
USER appuser
