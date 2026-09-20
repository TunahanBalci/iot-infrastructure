# syntax=docker/dockerfile:1

FROM python:3.13-slim AS build
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /src
COPY pyproject.toml ./
COPY src ./src
RUN python -m venv /opt/venv && /opt/venv/bin/pip install .

FROM python:3.13-slim
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PROCESSOR_CONFIG=/app/config/telemetry-processor.yaml
RUN useradd --system --uid 10001 --home-dir /app processor
COPY --from=build /opt/venv /opt/venv
WORKDIR /app
COPY config ./config
USER 10001
EXPOSE 8080

# Mount your own config over /app/config/telemetry-processor.yaml or override keys with PROCESSOR__* env vars.
# Writes nothing to disk: runs with readOnlyRootFilesystem.
ENTRYPOINT ["telemetry-processor"]
