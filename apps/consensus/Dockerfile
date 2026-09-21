# syntax=docker/dockerfile:1

FROM python:3.13-slim AS build
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /src
COPY pyproject.toml ./
COPY src ./src
# confluent-kafka wheels bundle librdkafka (SSL, SASL/SCRAM, lz4): no system packages needed.
RUN python -m venv /opt/venv && /opt/venv/bin/pip install . && /opt/venv/bin/pip uninstall -y pip setuptools

FROM python:3.13-slim
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CONSENSUS_CONFIG=/app/config/consensus.yaml
RUN useradd --system --uid 10001 --home-dir /app consensus \
 && mkdir -p /var/lib/consensus && chown 10001 /var/lib/consensus
COPY --from=build /opt/venv /opt/venv
WORKDIR /app
COPY config ./config
USER 10001
EXPOSE 8080

# Mount your own config over /app/config/consensus.yaml or override keys with CONSENSUS__* env vars.
# Kafka mode: CONSENSUS__INPUT__SOURCE=kafka CONSENSUS__OUTPUT__SINK=kafka CONSENSUS__KAFKA__SASL_PASSWORD=...
# with the cluster CA at /etc/app-kafka/ca.crt. Tracing: OTEL_EXPORTER_OTLP_ENDPOINT (empty = off).
ENTRYPOINT ["tunnel-consensus"]
