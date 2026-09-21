"""Central configuration: YAML file + CONSENSUS__* environment overrides, validated by pydantic."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ENV_PREFIX = "CONSENSUS__"
DEFAULT_CONFIG_PATH = Path("config/consensus.yaml")
POSITIONS = ("start", "middle", "end")
# Env override values of these keys stay strings (a numeric-looking password, "12:30" host ports, ...).
RAW_STRING_KEYS = frozenset({
    "host", "username", "password", "client_id_prefix",
    "bootstrap_servers", "sasl_username", "sasl_password", "ssl_ca_location", "client_id", "group_id",
    "detections_topic", "vehicles_topic", "traffic_topic", "geometry_topic",
})


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _check_range(v: tuple[float, float]) -> tuple[float, float]:
    if v[0] > v[1]:
        raise ValueError(f"range min > max: {v}")
    return v


class InputConfig(_Model):
    # mqtt: TBMQ APPLICATION client(s), tunnels partitioned by crc32 over worker processes.
    # kafka: consumer group member on kafka.detections_topic; tunnel ownership = assigned partitions.
    source: Literal["mqtt", "kafka"] = "mqtt"
    # Used when this service is a single partition: one wildcard subscription.
    topic_filter: str = "tunnels/+/sensors/+/detections"
    # Used when sharded: one subscription per owned tunnel.
    tunnel_topic_template: str = "tunnels/{tunnel_id}/sensors/+/detections"
    qos: Literal[0, 1, 2] = 1
    subscribe_batch: int = Field(500, ge=1, le=1500)  # filters per SUBSCRIBE packet (TBMQ max packet 64 KiB)

    @field_validator("tunnel_topic_template")
    @classmethod
    def _has_tunnel(cls, v: str) -> str:
        if "{tunnel_id}" not in v:
            raise ValueError("tunnel_topic_template must contain {tunnel_id}")
        return v


class PartitioningConfig(_Model):
    """Tunnels are owned by exactly one partition, so all three sensors of a tunnel meet
    in the same process. partitions = replicas * workers."""

    replicas: int = Field(1, ge=1)
    ordinal: int | Literal["auto"] = "auto"   # auto = trailing -N of $HOSTNAME (StatefulSet), else 0
    workers: int = Field(1, ge=1)
    # Tunnel id enumeration, needed only when partitions > 1 (MQTT wildcards cannot hash).
    tunnels: int = Field(25000, ge=1)
    index_offset: int = Field(0, ge=0)
    tunnel_id_format: str = "T{index:06d}"

    @property
    def partitions(self) -> int:
        return self.replicas * self.workers

    def resolved_ordinal(self, hostname: str | None = None) -> int:
        if self.ordinal != "auto":
            ordinal = self.ordinal
        else:
            m = re.search(r"-(\d+)$", hostname if hostname is not None else os.environ.get("HOSTNAME", ""))
            ordinal = int(m.group(1)) if m else 0
        if ordinal >= self.replicas:
            raise ValueError(f"ordinal {ordinal} >= replicas {self.replicas}")
        return ordinal


class TlsConfig(_Model):
    enabled: bool = False
    ca_certs: str | None = None
    certfile: str | None = None
    keyfile: str | None = None
    insecure: bool = False


class MqttConfig(_Model):
    host: str = "localhost"
    port: int = Field(1883, ge=1, le=65535)
    transport: Literal["tcp", "websockets"] = "tcp"
    # TBMQ APPLICATION clients need alphanumeric client ids (one Kafka topic per client).
    client_id_prefix: str = "consensus"
    username: str | None = None
    password: str | None = None
    keepalive_s: int = Field(30, ge=1)
    clean_start: bool = True
    session_expiry_s: int = Field(300, ge=0)  # > 0 = persistent session (survives broker/network blips)
    connect_timeout_s: float = Field(30.0, gt=0)
    reconnect_delay_s: tuple[int, int] = (1, 30)
    max_pending_messages: int = Field(100000, ge=1)  # outgoing buffer per connection; beyond = dropped
    status_topic_template: str = "consensus/{client_id}/status"
    tls: TlsConfig = TlsConfig()

    check_ranges = field_validator("reconnect_delay_s")(_check_range)

    @field_validator("client_id_prefix")
    @classmethod
    def _alnum(cls, v: str) -> str:
        if not v.isalnum():
            raise ValueError("client_id_prefix must be alphanumeric (TBMQ APPLICATION client id validation)")
        return v


KafkaScalar = str | int | float | bool


def _librdkafka_keys(v: Any) -> Any:
    """Passthrough maps: env overrides cannot contain dots, so fetch_min_bytes == fetch.min.bytes."""
    if isinstance(v, dict):
        return {str(k).lower().replace("_", "."): val for k, val in v.items()}
    return v


class KafkaConfig(_Model):
    """App Kafka client settings (input.source=kafka and/or output.sink=kafka). Defaults = cluster contract."""

    bootstrap_servers: str = "app-kafka-kafka-bootstrap.iot-pipeline.svc:9093"
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "SASL_SSL"
    sasl_mechanism: Literal["SCRAM-SHA-512", "SCRAM-SHA-256", "PLAIN"] = "SCRAM-SHA-512"
    sasl_username: str | None = "consensus"
    sasl_password: str | None = None
    ssl_ca_location: str | None = "/etc/app-kafka/ca.crt"   # cluster CA PEM
    client_id: str | None = None      # null = consensus-$HOSTNAME
    group_id: str = "consensus"
    detections_topic: str = "iot.detections"
    vehicles_topic: str = "iot.vehicles"
    traffic_topic: str = "iot.traffic"
    geometry_topic: str = "iot.consensus.geometry"
    commit_interval_s: float = Field(5.0, gt=0)          # flush producer + commit low watermarks
    poll_batch: int = Field(500, ge=1, le=100000)        # records per consume() call
    flush_timeout_s: float = Field(30.0, gt=0)           # producer flush before a commit
    geometry_load_timeout_s: float = Field(60.0, gt=0)   # read the geometry topic to its end
    # librdkafka properties applied last (keys with dots or underscores: fetch_min_bytes = fetch.min.bytes)
    consumer: dict[str, KafkaScalar] = {}
    producer: dict[str, KafkaScalar] = {}

    passthrough_keys = field_validator("consumer", "producer", mode="before")(_librdkafka_keys)

    def resolved_client_id(self, hostname: str | None = None) -> str:
        if self.client_id:
            return self.client_id
        host = hostname if hostname is not None else os.environ.get("HOSTNAME", "")
        return f"consensus-{host}" if host else "consensus"


class GeometryConfig(_Model):
    """Sensor spacing is learned per tunnel from traffic (no static map needed)."""

    # Upper bound must clear the longest tunnel in the deployment or it can never calibrate:
    # the half length has to land inside [length_m[0]/2, length_m[1]/2]. 25 km covers the
    # longest road tunnel there is (Laerdal, 24.5 km); Zigana and Ovit are 14.5/14.3 km.
    # Raising it widens the calibration anchor window, so keep it near the real maximum.
    length_m: tuple[float, float] = (100.0, 25000.0)  # plausible tunnel length range
    speed_kmh: tuple[float, float] = (20.0, 160.0)   # plausible vehicle speeds (bounds calibration window)
    calibration_min_length_m: float = Field(8.0, ge=0)  # only long vehicles vote: rarer and distinctive
    min_votes: float = Field(8.0, gt=0)              # weighted votes in the winning peak
    peak_ratio: float = Field(2.5, ge=1)             # winning peak vs best competing peak
    bin_ratio: float = Field(0.01, gt=0, lt=0.2)     # log-histogram bin width
    refine_alpha: float = Field(0.01, ge=0, le=1)    # EMA refinement from matched vehicles; 0 = off
    state_dir: str | None = None                     # persist learned lengths here (skip recalibration on restart)
    save_interval_s: float = Field(60.0, gt=0)

    check_ranges = field_validator("length_m", "speed_kmh")(_check_range)


class AssociationConfig(_Model):
    # Constant speed puts the middle detection at the midpoint of start and end: that
    # constraint only carries timestamp jitter, so it separates vehicles in dense traffic.
    midpoint_tolerance_ms: float = Field(150.0, gt=0)  # min gate on |t_middle - (t_start + t_end) / 2|
    time_tolerance_ms: float = Field(250.0, ge=0)      # fixed part of travel-time gates (jitter)
    time_tolerance_ratio: float = Field(0.03, ge=0)    # minimum relative travel-time gate (speed changes)
    gate_sigma: float = Field(3.0, gt=0)               # travel-time gate = sigma * learned speed noise
    speed_noise_prior: float = Field(0.03, gt=0, lt=1)  # relative speed error assumed for a new sensor
    length_noise_prior_m: float = Field(0.6, gt=0)     # length error assumed for a new sensor
    noise_alpha: float = Field(0.02, gt=0, le=1)       # learning rate of per-sensor speed/length noise
    max_lateness_ms: float = Field(2000.0, ge=0)       # wait this long past the expected last sensor
    dedup_window: int = Field(512, ge=8)               # recent seqs remembered per sensor


class FusionConfig(_Model):
    # Static weight of a sensor's vote, by position. The devices report their own health, so a
    # weight is a property of where a sensor sits, not of how well it has been voting.
    position_weights: dict[Literal["start", "middle", "end"], float] = Field(default_factory=dict)
    confidence_range: tuple[float, float] = (0.5, 0.999)

    check_ranges = field_validator("confidence_range")(_check_range)

    @field_validator("position_weights")
    @classmethod
    def _positive(cls, v: dict[str, float]) -> dict[str, float]:
        if any(w <= 0 for w in v.values()):
            raise ValueError("position weights must be > 0")
        return v


class ReportingConfig(_Model):
    interval_s: float | None = Field(60.0, gt=0)  # per-tunnel traffic window; null = off


class OutputConfig(_Model):
    sink: Literal["mqtt", "kafka", "stdout", "discard"] = "mqtt"
    vehicle_topic_template: str = "tunnels/{tunnel_id}/vehicles"
    traffic_topic_template: str = "tunnels/{tunnel_id}/traffic"
    qos: Literal[0, 1, 2] = 0
    message_expiry_s: int | None = Field(300, ge=0)
    include_detections: bool = True  # message ids of the fused detections (traceability)


class ServiceConfig(_Model):
    stats_interval_s: float = Field(5.0, gt=0)
    http_port: int | None = Field(8080, ge=1, le=65535)  # /healthz /readyz /metrics; null = off


class LoggingConfig(_Model):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"


class Config(_Model):
    input: InputConfig = InputConfig()
    partitioning: PartitioningConfig = PartitioningConfig()
    mqtt: MqttConfig = MqttConfig()
    kafka: KafkaConfig = KafkaConfig()
    geometry: GeometryConfig = GeometryConfig()
    association: AssociationConfig = AssociationConfig()
    fusion: FusionConfig = FusionConfig()
    reporting: ReportingConfig = ReportingConfig()
    output: OutputConfig = OutputConfig()
    service: ServiceConfig = ServiceConfig()
    logging: LoggingConfig = LoggingConfig()

    @model_validator(mode="after")
    def _templates(self) -> Config:
        try:
            self.output.vehicle_topic_template.format(tunnel_id="x")
            self.output.traffic_topic_template.format(tunnel_id="x")
            self.partitioning.tunnel_id_format.format(index=0)
        except (KeyError, IndexError) as e:
            raise ValueError(f"unknown placeholder {e} in a topic template or tunnel_id_format") from e
        if self.input.source == "kafka" and self.output.sink == "mqtt":
            raise ValueError("input.source=kafka needs output.sink kafka, stdout or discard")
        return self

    @property
    def uses_kafka(self) -> bool:
        return self.input.source == "kafka" or self.output.sink == "kafka"


def apply_env_overrides(data: dict[str, Any], environ: dict[str, str] | None = None) -> dict[str, Any]:
    """Apply CONSENSUS__A__B=value overrides onto nested dict `data` (in place, returned)."""
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
        if raw == "":
            node[path[-1]] = None
        elif path[-1] in RAW_STRING_KEYS:
            node[path[-1]] = raw  # a numeric-looking password must stay a string
        else:
            node[path[-1]] = yaml.safe_load(raw)
    return data


def load_config(path: str | Path | None = None, environ: dict[str, str] | None = None) -> Config:
    env = os.environ if environ is None else environ
    if path is None:
        path = env.get("CONSENSUS_CONFIG") or DEFAULT_CONFIG_PATH
    path = Path(path)
    data: dict[str, Any] = {}
    if path.exists():
        with path.open() as f:
            data = yaml.safe_load(f) or {}
    elif "CONSENSUS_CONFIG" in env or path != DEFAULT_CONFIG_PATH:
        raise FileNotFoundError(f"config file not found: {path}")
    apply_env_overrides(data, env)
    return Config.model_validate(data)
