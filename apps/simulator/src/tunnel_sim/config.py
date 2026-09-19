"""Central configuration: YAML file + SIM__* environment overrides, validated by pydantic."""

from __future__ import annotations

import os
import re
import string
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator, model_validator

from .profiles import TimeWindow, TunnelProfile, load_profiles

ENV_PREFIX = "SIM__"
DEFAULT_CONFIG_PATH = Path("config/simulator.yaml")
POSITIONS = ("start", "middle", "end")


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _check_range(v: tuple[float, float]) -> tuple[float, float]:
    if v[0] > v[1]:
        raise ValueError(f"range min > max: {v}")
    return v


class SimulationConfig(_Model):
    seed: int | None = 42
    time_scale: float = Field(1.0, gt=0)
    duration_s: float | None = Field(None, gt=0)
    warm_start: bool = True
    workers: int | Literal["auto"] = "auto"
    tick_ms: float = Field(2.0, ge=0, le=100)
    stats_interval_s: float = Field(5.0, gt=0)
    max_lag_s: float = Field(5.0, gt=0)
    # A laptop that suspends freezes CLOCK_MONOTONIC but not the wall clock, so the event
    # clock silently falls behind by the sleep duration and every payload ts is stamped in
    # the past. Re-anchor once the gap exceeds this; 0 disables (a frozen host is then visible
    # as stale data instead of a jump). Ignored when time_scale != 1: sim time is meant to
    # diverge there. Tune up if a loaded host's scheduling jitter causes needless re-anchors.
    resync_threshold_s: float = Field(2.0, ge=0)

    def resolved_workers(self) -> int:
        if self.workers != "auto":
            return max(1, self.workers)
        try:
            cpus = len(os.sched_getaffinity(0))
        except AttributeError:  # non-Linux
            cpus = os.cpu_count() or 1
        return max(1, min(cpus, 16))


class TopologyConfig(_Model):
    # profiles:  the curated tunnels of profiles_path (the deployment)
    # generator: tunnels synthesized from the seed (load tests, make sim-up MODE=generator)
    mode: Literal["profiles", "generator"] = "profiles"
    profiles_path: str = "config/tunnels.yaml"
    tunnels: int = Field(25000, ge=1)
    index_offset: int = Field(0, ge=0)
    tunnel_id_format: str = "T{index:06d}"
    length_m: tuple[float, float] = (300.0, 3000.0)
    lanes_per_direction: tuple[int, int] = (1, 3)

    check_ranges = field_validator("length_m", "lanes_per_direction")(_check_range)

    @field_validator("length_m")
    @classmethod
    def _positive(cls, v: tuple[float, float]) -> tuple[float, float]:
        if v[0] <= 0:
            raise ValueError("tunnel length must be > 0")
        return v


class Distribution(_Model):
    mean: float
    std: float = Field(ge=0)
    min: float
    max: float

    @model_validator(mode="after")
    def _bounds(self) -> Distribution:
        if self.min > self.max:
            raise ValueError("min > max")
        return self


class BurstConfig(_Model):
    """Time of day when every device sends faster (local Istanbul time)."""

    start: str = Field("17:00", alias="from")
    end: str = Field("20:00", alias="to")
    msgs_per_device_s: float = Field(3.0, gt=0)

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    @model_validator(mode="after")
    def _parse(self) -> BurstConfig:
        self._win = TimeWindow.parse(self.start, self.end)
        return self

    _win: TimeWindow = PrivateAttr()

    @property
    def window(self) -> TimeWindow:
        return self._win


class TrafficConfig(_Model):
    """Message rate and direction split. One vehicle produces one detection per device, so the
    per-device rate is also the per-tunnel vehicle arrival rate. Vehicle dimensions and free
    speeds come from the type (vehicles.PHYSICAL) and the tunnel's speed limit."""

    msgs_per_device_s: float = Field(2.0, gt=0)
    # Load tests state a total instead: msgs_per_device_s becomes target / (tunnels * 3 devices).
    # make sim-up MODE=generator LOAD=50000 sets it.
    target_total_msgs_s: float | None = Field(None, gt=0)
    burst: BurstConfig = BurstConfig()
    rate_variation: tuple[float, float] = (1.0, 1.0)   # per-tunnel multiplier (load tests)
    start_to_end_ratio: float = Field(0.5, ge=0, le=1)
    # How much of a banned type still enters the tunnel during its banned hours. Not zero:
    # those few vehicles are exactly what the restricted-vehicle alerts report.
    violation_factor: float = Field(0.02, ge=0, le=1)
    speeding_per_day: tuple[float, float] = (2.0, 3.0)   # vehicles over the limit, per tunnel per day

    check_ranges = field_validator("rate_variation", "speeding_per_day")(_check_range)

    def expected_daily_vehicles(self, rate_multiplier: float = 1.0) -> float:
        burst_s = self.burst.window.duration_minutes * 60
        return rate_multiplier * (self.msgs_per_device_s * (86400 - burst_s)
                                  + self.burst.msgs_per_device_s * burst_s)

    def with_total(self, devices: int) -> TrafficConfig:
        """This config with msgs_per_device_s resolved from target_total_msgs_s, if one is set."""
        if self.target_total_msgs_s is None or devices <= 0:
            return self
        return self.model_copy(update={"msgs_per_device_s": self.target_total_msgs_s / devices})

    def rate_at(self, minute_of_day: int) -> float:
        """Messages per second per device at that local minute."""
        b = self.burst
        return b.msgs_per_device_s if b.window.contains(minute_of_day // 60, minute_of_day % 60) \
            else self.msgs_per_device_s


class SensorErrorModel(_Model):
    """Fields that may be overridden per sensor position."""

    length_noise_std_m: float = Field(0.6, ge=0)
    misclassification_rate: float = Field(0.03, ge=0, le=1)
    unknown_rate: float = Field(0.01, ge=0, le=1)
    miss_rate: float = Field(0.01, ge=0, le=1)
    duplicate_rate: float = Field(0.005, ge=0, le=1)
    timestamp_jitter_ms: float = Field(15.0, ge=0)
    speed_noise_ratio: float = Field(0.03, ge=0, lt=1)


class SensorOverride(_Model):
    length_noise_std_m: float | None = Field(None, ge=0)
    misclassification_rate: float | None = Field(None, ge=0, le=1)
    unknown_rate: float | None = Field(None, ge=0, le=1)
    miss_rate: float | None = Field(None, ge=0, le=1)
    duplicate_rate: float | None = Field(None, ge=0, le=1)
    timestamp_jitter_ms: float | None = Field(None, ge=0)
    speed_noise_ratio: float | None = Field(None, ge=0, lt=1)


class SensorsConfig(SensorErrorModel):
    duplicate_delay_ms: tuple[float, float] = (5.0, 500.0)
    degraded_ratio: float = Field(0.05, ge=0, le=1)
    degraded_multiplier: float = Field(5.0, ge=1)
    overrides: dict[Literal["start", "middle", "end"], SensorOverride] = Field(default_factory=dict)

    check_ranges = field_validator("duplicate_delay_ms")(_check_range)

    def error_model(self, position: str, degraded: bool) -> SensorErrorModel:
        """Effective error model for a sensor at `position`, optionally degraded."""
        base = {k: getattr(self, k) for k in SensorErrorModel.model_fields}
        override = self.overrides.get(position)  # type: ignore[call-overload]
        if override is not None:
            base.update(override.model_dump(exclude_none=True))
        if degraded:
            m = self.degraded_multiplier
            for k in ("misclassification_rate", "unknown_rate", "miss_rate", "duplicate_rate"):
                base[k] = min(1.0, base[k] * m)
            for k in ("length_noise_std_m", "timestamp_jitter_ms"):
                base[k] = base[k] * m
            base["speed_noise_ratio"] = min(0.5, base["speed_noise_ratio"] * m)
        return SensorErrorModel(**base)


class PayloadConfig(_Model):
    include_vehicle_id: bool = False
    include_true_class: bool = False


def _format_fields(template: str) -> set[str]:
    """Top-level placeholder names of a str.format template (empty if it is not a valid template)."""
    try:
        return {re.split(r"[.\[]", f, maxsplit=1)[0]
                for _, f, _, _ in string.Formatter().parse(template) if f is not None}
    except ValueError:
        return set()


class TlsConfig(_Model):
    enabled: bool = False
    ca_certs: str | None = None
    certfile: str | None = None
    keyfile: str | None = None
    insecure: bool = False

    def files_for(self, tunnel_id: str) -> tuple[str | None, str | None]:
        """certfile/keyfile of one tunnel's client (per_tunnel mode), with {tunnel_id} filled in."""
        return (None if self.certfile is None else self.certfile.format(tunnel_id=tunnel_id),
                None if self.keyfile is None else self.keyfile.format(tunnel_id=tunnel_id))


class MqttConfig(_Model):
    host: str = "localhost"
    port: int = Field(1883, ge=1, le=65535)
    transport: Literal["tcp", "websockets"] = "tcp"
    connection_mode: Literal["shared", "per_tunnel"] = "shared"
    client_id_prefix: str = "tunnel-sim"
    username: str | None = None
    password: str | None = None
    keepalive_s: int = Field(30, ge=1)
    qos: Literal[0, 1, 2] = 0
    retain: bool = False
    connections_per_worker: int = Field(1, ge=1)
    topic_template: str = "tunnels/{tunnel_id}/sensors/{position}/detections"
    health_topic_template: str = "tunnels/{tunnel_id}/sensors/{position}/health"
    status_topic_template: str = "simulator/{client_id}/status"
    message_expiry_s: int | None = Field(60, ge=0)
    user_properties: bool = True
    session_expiry_s: int = Field(0, ge=0)
    max_pending_messages: int = Field(200000, ge=1)
    connect_rate_per_s: float = Field(500.0, gt=0)
    connect_timeout_s: float = Field(30.0, gt=0)
    reconnect_delay_s: tuple[int, int] = (1, 30)
    tls: TlsConfig = TlsConfig()

    check_ranges = field_validator("reconnect_delay_s")(_check_range)

    @model_validator(mode="after")
    def _tls_file_placeholders(self) -> MqttConfig:
        for key in ("certfile", "keyfile"):
            path = getattr(self.tls, key)
            if path is None:
                continue
            if self.connection_mode == "shared":
                if "tunnel_id" in _format_fields(path):
                    raise ValueError(f"tls.{key} contains {{tunnel_id}}, which requires connection_mode: per_tunnel")
                continue
            try:
                path.format(tunnel_id="x")
            except (KeyError, IndexError, AttributeError) as e:
                raise ValueError(f"tls.{key}: unknown placeholder {e}; allowed: tunnel_id") from e
        return self

    @field_validator("topic_template", "health_topic_template")
    @classmethod
    def _topic_placeholders(cls, v: str) -> str:
        try:
            v.format(tunnel_id="x", position="x", sensor_id="x")
        except (KeyError, IndexError) as e:
            raise ValueError(f"unknown placeholder {e}; allowed: tunnel_id, position, sensor_id") from e
        return v


class OutputConfig(_Model):
    sink: Literal["mqtt", "stdout", "discard"] = "mqtt"


class LoggingConfig(_Model):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"


class ServiceConfig(_Model):
    http_port: int | None = Field(None, ge=1, le=65535)   # Prometheus /metrics, /healthz, /time; null = off
    http_bind: str = "0.0.0.0"                            # make sim-up binds the host-local node address
    health_interval_s: float = Field(60.0, gt=0)          # device health window (metrics + retained MQTT)
    # Per-device Prometheus series are only useful while there are few devices; a load test with
    # 25000 tunnels would add 75000 series per metric, so they switch off past this many devices.
    device_metrics_max: int = Field(300, ge=0)

    def device_metrics_enabled(self, tunnel_count: int) -> bool:
        return tunnel_count * 3 <= self.device_metrics_max


class Config(_Model):
    profiles: list[TunnelProfile] = Field(default_factory=list)   # filled by load_config in profiles mode
    simulation: SimulationConfig = SimulationConfig()
    topology: TopologyConfig = TopologyConfig()
    traffic: TrafficConfig = TrafficConfig()
    sensors: SensorsConfig = SensorsConfig()
    payload: PayloadConfig = PayloadConfig()
    mqtt: MqttConfig = MqttConfig()
    output: OutputConfig = OutputConfig()
    service: ServiceConfig = ServiceConfig()
    logging: LoggingConfig = LoggingConfig()

    def tunnel_count(self) -> int:
        return len(self.profiles) if self.profiles else self.topology.tunnels

    @model_validator(mode="after")
    def _resolve_target_load(self) -> Config:
        """Config built directly (tests, defaults) resolves a target load too; load_config repeats
        it after the profiles are known."""
        object.__setattr__(self, "traffic", self.traffic.with_total(self.tunnel_count() * 3))
        return self

    def expected_msgs_per_s(self) -> float:
        """Approximate steady-state detection message rate outside the burst (ignores degraded sensors)."""
        rv = self.traffic.rate_variation
        vehicles_s = self.tunnel_count() * self.traffic.msgs_per_device_s * (rv[0] + rv[1]) / 2
        s = self.sensors
        per_vehicle = 0.0
        for pos in POSITIONS:
            em = s.error_model(pos, degraded=False)
            per_vehicle += (1 - em.miss_rate) * (1 + em.duplicate_rate)
        return vehicles_s * per_vehicle


def _parse_env_value(raw: str) -> Any:
    """YAML scalar or list. Values containing braces stay strings, so format templates such as
    /certs/{tunnel_id}.pem never turn into YAML flow mappings (or parse errors)."""
    if raw == "":
        return None
    if "{" in raw or "}" in raw:
        try:
            value = yaml.safe_load(raw)
        except yaml.YAMLError:
            return raw
        return value if isinstance(value, str) else raw  # a quoted template still gets unquoted
    return yaml.safe_load(raw)


def apply_env_overrides(data: dict[str, Any], environ: dict[str, str] | None = None) -> dict[str, Any]:
    """Apply SIM__A__B=value overrides onto nested dict `data` (in place, returned)."""
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
        node[path[-1]] = _parse_env_value(raw)
    return data


def load_config(path: str | Path | None = None, environ: dict[str, str] | None = None) -> Config:
    env = os.environ if environ is None else environ
    if path is None:
        path = env.get("SIM_CONFIG") or DEFAULT_CONFIG_PATH
    path = Path(path)
    data: dict[str, Any] = {}
    if path.exists():
        with path.open() as f:
            data = yaml.safe_load(f) or {}
    elif "SIM_CONFIG" in env or path != DEFAULT_CONFIG_PATH:
        raise FileNotFoundError(f"config file not found: {path}")
    apply_env_overrides(data, env)
    cfg = Config.model_validate(data)
    if cfg.topology.mode == "profiles":
        cfg.profiles = load_profiles(cfg.topology.profiles_path)
    cfg.traffic = cfg.traffic.with_total(cfg.tunnel_count() * 3)
    return cfg
