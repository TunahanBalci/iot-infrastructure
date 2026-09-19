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
    SIM_CONFIG=/app/config/simulator.yaml
RUN useradd --system --uid 10001 --home-dir /app sim
COPY --from=build /opt/venv /opt/venv
WORKDIR /app
COPY config ./config
USER sim

# Mount your own config over /app/config/simulator.yaml or override keys with SIM__* env vars.
ENTRYPOINT ["tunnel-sim"]
