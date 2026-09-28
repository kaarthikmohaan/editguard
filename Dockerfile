# EditGuard runtime images (design: "build images for producer, jobs, enricher and API").
#   producer  EventStreams -> Kafka (edits and baseline streams); no Java
#   spark     live and replay Spark jobs; Java 17 and the connector jars baked in
# Build: make images. Released images are pushed to GHCR by the release workflow (M3 step 9b).

FROM ghcr.io/astral-sh/uv:0.12.19 AS uv

FROM python:3.12-slim-bookworm AS base
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1
RUN useradd --create-home --uid 1000 app
WORKDIR /opt/editguard
# Dependencies first (cached until uv.lock changes), then the project itself.
COPY pyproject.toml uv.lock README.md ./
# Avro schemas read at runtime, relative to the project root (contracts/generated).
COPY contracts/generated ./contracts/generated

FROM base AS producer
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-install-project --no-install-package pyspark
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-install-package pyspark
USER app
ENTRYPOINT ["python", "-m", "editguard.producer"]

FROM base AS spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev
USER app
# Download the Iceberg, Kafka and Avro connector jars now, so containers start offline and fast.
RUN python -c "from pyspark.sql import SparkSession; \
from editguard.streaming.live_job import PACKAGES; \
SparkSession.builder.master('local[1]').config('spark.jars.packages', PACKAGES).getOrCreate().stop()"
ENTRYPOINT ["python", "-m", "editguard.streaming.live_job"]
