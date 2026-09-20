"""Synthetic TBMQ envelopes and a Kafka-free throughput benchmark of the processing path.

`telemetry-processor --benchmark [N]` runs:
  process      normalize.process() only (JSON + base64 + validation + output JSON)
  batch        the service's batch loop with in-memory consumer records and a discarding producer,
               tracing off, then tracing on (parentbased_traceidratio 0 and 0.01, in-memory exporter)
"""

from __future__ import annotations

import base64
import os
import random
import time

import orjson

from .config import Config
from .metrics import Stats
from .normalize import SCHEMA_VERSION

POSITIONS = ("start", "middle", "end")


def device_id_for(sensor_id: str) -> str:
    """Stable synthetic MAC for a test/benchmark device (the simulator derives the real one)."""
    h = abs(hash(sensor_id)) % (1 << 40)
    return "02:" + ":".join(f"{(h >> (8 * i)) & 0xFF:02x}" for i in range(4, -1, -1))


def make_detection(tunnel_id: str = "T000042", position: str = "middle", seq: int = 1234,
                   ts: int = 1789377527984, **overrides) -> dict:
    """Simulator schema 2 detection (see apps/simulator payload.py)."""
    sensor_id = f"{tunnel_id}-{position}"
    d = {"schema": SCHEMA_VERSION, "message_id": f"{sensor_id}:9f3a1c:{seq}", "sensor_id": sensor_id,
         "device_id": device_id_for(sensor_id), "device_serial": "TSN-QUT73ELQA5",
         "tunnel_id": tunnel_id, "position": position, "seq": seq, "ts": ts, "direction": "end_to_start", "lane": 1,
         "plate": "34 ABC 123", "speed_kmh": 66.2, "length_m": 16.02, "occupancy_ms": 871,
         "classification": "KAMYON", "confidence": 0.99}
    d.update(overrides)
    return d


def make_health(tunnel_id: str = "T000042", position: str = "middle", ts: int = 1789377527984,
                **overrides) -> dict:
    """Device health report (see apps/simulator payload.py: health_payload)."""
    sensor_id = f"{tunnel_id}-{position}"
    h = {"schema": SCHEMA_VERSION, "sensor_id": sensor_id, "device_id": device_id_for(sensor_id),
         "device_serial": "TSN-QUT73ELQA5", "tunnel_id": tunnel_id, "position": position, "ts": ts,
         "status": "ok", "degraded": False, "window_s": 60.0, "uptime_s": 3600.0,
         "detections": 118, "duplicates": 1, "missed": 2}
    h.update(overrides)
    return h


def make_envelope(detection: dict | bytes | None = None, *, tunnel_id: str | None = None,
                  position: str | None = None, topic: str | None = None, cn: object = "__tunnel__",
                  ts: int = 1789377527990, raw_payload: object = None, **overrides) -> dict:
    """TBMQ integration-executor envelope around a detection (dict or raw payload bytes)."""
    if detection is None:
        detection = make_detection(**({"tunnel_id": tunnel_id} if tunnel_id else {}),
                                   **({"position": position} if position else {}))
    if isinstance(detection, dict):
        tunnel_id = tunnel_id or detection.get("tunnel_id", "T000042")
        position = position or detection.get("position", "middle")
        payload_bytes = orjson.dumps(detection)
    else:
        tunnel_id = tunnel_id or "T000042"
        position = position or "middle"
        payload_bytes = detection
    env = {
        "payload": base64.b64encode(payload_bytes).decode() if raw_payload is None else raw_payload,
        "topicName": topic if topic is not None else f"tunnels/{tunnel_id}/sensors/{position}/detections",
        "clientId": f"tunnel-sim-{tunnel_id}", "eventType": "PUBLISH_MSG", "qos": 0, "retain": False,
        "tbmqIeNode": "tbmq-integration-executor-0", "tbmqNode": "tbmq-0", "ts": ts,
        "clientCertCn": tunnel_id if cn == "__tunnel__" else cn,
        "props": {"tunnel_id": tunnel_id, "sensor_id": f"{tunnel_id}-{position}", "position": position,
                  "device_id": device_id_for(f"{tunnel_id}-{position}"), "schema": str(SCHEMA_VERSION)},
        "metadata": {},
    }
    if cn is None:
        del env["clientCertCn"]
    env.update(overrides)
    return env


def synthetic_values(n: int, tunnels: int = 1000, invalid_ratio: float = 0.02, seed: int = 1) -> list[bytes]:
    rng = random.Random(seed)
    now = int(time.time() * 1000)
    out = []
    for i in range(n):
        tunnel_id = f"T{rng.randrange(tunnels):06d}"
        position = POSITIONS[i % 3]
        d = make_detection(tunnel_id, position, seq=i, ts=now - rng.randrange(200),
                           direction=rng.choice(("start_to_end", "end_to_start")), lane=rng.randrange(1, 3),
                           speed_kmh=round(rng.uniform(40, 120), 1), length_m=round(rng.uniform(3, 18), 2),
                           occupancy_ms=rng.randrange(100, 1500),
                           classification=rng.choice(("OTOMOBIL", "KAMYON", "MINIBUS", "CEKICI_YARI_ROMORK")),
                           confidence=round(rng.uniform(0.5, 1.0), 3))
        env = make_envelope(d, ts=now)
        if rng.random() < invalid_ratio:
            env["clientCertCn"] = "T999999"  # identity_mismatch: still fully parsed
        out.append(orjson.dumps(env))
    return out


class FakeMessage:
    __slots__ = ("_value", "_partition", "_offset", "_headers", "_topic")

    def __init__(self, value: bytes | None, partition: int = 0, offset: int = 0, headers=None,
                 topic: str = "iot.mqtt.ingest"):
        self._value, self._partition, self._offset, self._headers, self._topic = value, partition, offset, headers, topic

    def error(self):
        return None

    def value(self):
        return self._value

    def headers(self):
        return self._headers

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def topic(self):
        return self._topic

    def key(self):
        return None


class DiscardProducer:
    """Acknowledges everything on poll(); keeps nothing."""

    def __init__(self, conf: dict | None = None):
        self.callbacks: list = []

    def produce(self, topic, value=None, key=None, on_delivery=None, headers=None):
        self.callbacks.append(on_delivery)

    def poll(self, timeout: float = 0) -> int:
        cbs, self.callbacks = self.callbacks, []
        for cb in cbs:
            cb(None, None)
        return len(cbs)

    def flush(self, timeout: float = 0) -> int:
        self.poll()
        return 0

    def __len__(self) -> int:
        return len(self.callbacks)


def _rate(n: int, fn) -> tuple[float, float]:
    t0, c0 = time.perf_counter(), time.process_time()
    fn()
    wall, cpu = time.perf_counter() - t0, time.process_time() - c0
    return n / wall, n / cpu


def run_benchmark(n: int, batch_size: int = 500) -> int:
    from . import normalize
    from .service import Service

    values = synthetic_values(n)
    msgs = [FakeMessage(v, i % 6, i) for i, v in enumerate(values)]
    now_ms = int(time.time() * 1000)
    print(f"{n:,} synthetic envelopes, {sum(map(len, values)) / n:.0f} bytes avg, 2% invalid")

    def process_only():
        process = normalize.process
        for v in values:
            process(v, now_ms)

    def batches(service):
        def run():
            for i in range(0, n, batch_size):
                service._process_batch(msgs[i:i + batch_size])
                service.producer.poll(0)
                service._collect()
        return run

    process_only()  # warm up
    wall, cpu = _rate(n, process_only)
    print(f"process()              {wall:>10,.0f} envelopes/s (cpu {cpu:,.0f}/s)")

    cfg = Config()
    service = Service(cfg, Stats(), None, consumer_factory=lambda conf: None, producer_factory=DiscardProducer)
    wall, cpu = _rate(n, batches(service))
    print(f"batch loop, no tracing {wall:>10,.0f} envelopes/s (cpu {cpu:,.0f}/s)")

    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    from .tracing import Tracing
    for ratio in ("0", "0.01"):
        env = dict(os.environ, OTEL_TRACES_SAMPLER="parentbased_traceidratio", OTEL_TRACES_SAMPLER_ARG=ratio)
        exporter = InMemorySpanExporter()
        tracing = Tracing(input_topic=cfg.kafka.input_topic, group_id=cfg.kafka.group_id, exporter=exporter,
                          environ=env)
        service = Service(cfg, Stats(), tracing, consumer_factory=lambda conf: None, producer_factory=DiscardProducer)
        wall, cpu = _rate(n, batches(service))
        label = f"tracing {float(ratio):.0%}"
        print(f"batch loop, {label:<11}{wall:>10,.0f} envelopes/s (cpu {cpu:,.0f}/s), "
              f"{len(exporter.get_finished_spans()):,} spans")
    return 0
