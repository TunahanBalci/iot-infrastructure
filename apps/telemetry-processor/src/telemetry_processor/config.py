"""Central configuration: YAML file + PROCESSOR__* environment overrides, validated by pydantic."""

from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ENV_PREFIX = "PROCESSOR__"
CONFIG_ENV = "PROCESSOR_CONFIG"
DEFAULT_CONFIG_PATH = Path("config/telemetry-processor.yaml")
# Values that must stay strings even when they look like YAML numbers/booleans.
RAW_STRING_KEYS = frozenset({"bootstrap_servers", "sasl_username", "sasl_password", "ssl_ca_location", "client_id",
                             "group_id", "input_topic", "output_topic", "health_topic", "rejected_topic"})
# librdkafka passthrough maps: every value stays a string, keys may use _ instead of . (env var friendly).
PASSTHROUGH_MAPS = frozenset({"consumer", "producer"})


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _librdkafka_properties(v: dict[str, Any] | None) -> dict[str, str]:
    """{"fetch_wait_max_ms": 100, "check.crcs": True} -> {"fetch.wait.max.ms": "100", "check.crcs": "true"}.
    librdkafka property names never contain underscores, so _ -> . is unambiguous."""
    out = {}
    for key, value in (v or {}).items():
        if isinstance(value, bool):
            value = "true" if value else "false"
        out[str(key).lower().replace("_", ".")] = str(value)
    return out


class KafkaConfig(_Model):
    bootstrap_servers: str = "localhost:9092"
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"
    sasl_mechanism: Literal["SCRAM-SHA-512", "SCRAM-SHA-256", "PLAIN"] = "SCRAM-SHA-512"
    sasl_username: str | None = None
    sasl_password: str | None = None
    ssl_ca_location: str | None = None       # PEM CA bundle; null = system CAs
    client_id: str = "telemetry-processor-{hostname}"   # {hostname} = pod name
    group_id: str = "telemetry-processor"
    input_topic: str = "iot.mqtt.ingest"
    output_topic: str = "iot.detections"
    health_topic: str = "iot.sensor-health"          # device health reports (tunnels/+/sensors/+/health)
    rejected_topic: str = "iot.detections.rejected"
    poll_batch: int = Field(500, ge=1, le=100000)    # max records per poll batch
    poll_timeout_s: float = Field(0.1, gt=0)         # a batch waits at most this long to fill up
    commit_interval_s: float = Field(1.0, ge=0)      # commit acknowledged batches at most this often
    shutdown_timeout_s: float = Field(20.0, gt=0)    # producer flush on SIGTERM / partition revoke
    consumer: dict[str, str] = {}                    # extra librdkafka consumer properties (override built-ins)
    producer: dict[str, str] = {}                    # extra librdkafka producer properties (override built-ins)

    normalize_passthrough = field_validator("consumer", "producer", mode="before")(_librdkafka_properties)

    @model_validator(mode="after")
    def _check(self) -> KafkaConfig:
        if self.security_protocol.startswith("SASL_") and not (self.sasl_username and self.sasl_password):
            raise ValueError(f"security_protocol {self.security_protocol} needs sasl_username and sasl_password")
        if len({self.input_topic, self.output_topic, self.rejected_topic}) != 3:
            raise ValueError("input_topic, output_topic and rejected_topic must differ")
        try:
            self.client_id.format(hostname="x")
        except (KeyError, IndexError) as e:
            raise ValueError(f"unknown placeholder {e} in client_id") from e
        return self

    def resolved_client_id(self, hostname: str | None = None) -> str:
        return self.client_id.format(hostname=hostname or os.environ.get("HOSTNAME") or socket.gethostname())


class ServiceConfig(_Model):
    http_port: int | None = Field(8080, ge=1, le=65535)  # /healthz /readyz /metrics; null = off
    stats_interval_s: float = Field(10.0, gt=0)


class LoggingConfig(_Model):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"


class Config(_Model):
    kafka: KafkaConfig = KafkaConfig()
    service: ServiceConfig = ServiceConfig()
    logging: LoggingConfig = LoggingConfig()

    def redacted(self) -> dict[str, Any]:
        data = self.model_dump(mode="json")
        if data["kafka"]["sasl_password"]:
            data["kafka"]["sasl_password"] = "***"
        return data


def apply_env_overrides(data: dict[str, Any], environ: dict[str, str] | None = None) -> dict[str, Any]:
    """Apply PROCESSOR__A__B=value overrides onto nested dict `data` (in place, returned)."""
    environ = os.environ if environ is None else environ
    for key, raw in environ.items():
        if not key.startswith(ENV_PREFIX):
            continue
        path = [p.lower() for p in key[len(ENV_PREFIX):].split("__") if p]
        if not path:
            continue
        node = data
        for part in path[:-1]:
            child = node.get(part)
            if not isinstance(child, dict):
                child = {}
                node[part] = child
            node = child
        if len(path) >= 2 and path[-2] in PASSTHROUGH_MAPS:
            node[path[-1]] = raw  # librdkafka property: always a string
        elif raw == "":
            node[path[-1]] = None
        elif path[-1] in RAW_STRING_KEYS:
            node[path[-1]] = raw  # a numeric-looking password must stay a string
        else:
            node[path[-1]] = yaml.safe_load(raw)
    return data


def load_config(path: str | Path | None = None, environ: dict[str, str] | None = None) -> Config:
    env = os.environ if environ is None else environ
    if path is None:
        path = env.get(CONFIG_ENV) or DEFAULT_CONFIG_PATH
    path = Path(path)
    data: dict[str, Any] = {}
    if path.exists():
        with path.open() as f:
            data = yaml.safe_load(f) or {}
    elif CONFIG_ENV in env or path != DEFAULT_CONFIG_PATH:
        raise FileNotFoundError(f"config file not found: {path}")
    apply_env_overrides(data, env)
    return Config.model_validate(data)
