"""App Kafka clients: configs, keyed outputs, the compacted geometry topic."""

from __future__ import annotations

import logging
import threading
import time
from bisect import bisect_left
from collections.abc import Callable

import orjson
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, Producer, TopicPartition

from .config import Config, KafkaConfig
from .model import Detection

log = logging.getLogger(__name__)

AGE_BUCKETS_S = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 900.0)


# --- client configs ------------------------------------------------------------

def base_config(k: KafkaConfig, client_id: str) -> dict:
    conf: dict = {
        "bootstrap.servers": k.bootstrap_servers,
        "security.protocol": k.security_protocol,
        "client.id": client_id,
        "error_cb": _log_client_error,
    }
    if k.security_protocol.startswith("SASL"):
        if not k.sasl_username or k.sasl_password is None:
            raise ValueError(f"kafka.security_protocol={k.security_protocol} needs kafka.sasl_username and "
                             "kafka.sasl_password (CONSENSUS__KAFKA__SASL_PASSWORD)")
        conf.update({"sasl.mechanism": k.sasl_mechanism, "sasl.username": k.sasl_username,
                     "sasl.password": k.sasl_password})
    if k.security_protocol.endswith("SSL") and k.ssl_ca_location:
        conf["ssl.ca.location"] = k.ssl_ca_location
    return conf


def consumer_config(k: KafkaConfig, client_id: str) -> dict:
    conf = base_config(k, client_id)
    conf.update({
        "group.id": k.group_id,
        # Only moved partitions are revoked on a rebalance; the others keep their state.
        "partition.assignment.strategy": "cooperative-sticky",
        "enable.auto.commit": False,         # low watermarks are committed explicitly
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",     # a new group loses nothing still retained
        "queued.max.messages.kbytes": 16384,  # prefetch buffer per consumer (memory bound)
    })
    conf.update(k.consumer)
    return conf


def producer_config(k: KafkaConfig, client_id: str) -> dict:
    conf = base_config(k, client_id)
    conf.update({
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "partitioner": "murmur2_random",      # Java-compatible: same tunnel -> same partition everywhere
        "linger.ms": 20,
        "queue.buffering.max.kbytes": 32768,  # produce() blocks (backpressure) beyond this
        "delivery.report.only.error": True,   # the delivery callback only runs for failures
    })
    conf.update(k.producer)
    return conf


def _log_client_error(err: KafkaError) -> None:
    if err.fatal():
        log.error("kafka fatal error: %s", err)
    else:
        log.warning("kafka: %s", err)


# --- metrics shared by the Kafka parts ---------------------------------------------

class KafkaMetrics:
    def __init__(self) -> None:
        self.produce_errors = 0
        self.produce_backpressure = 0
        self.age_counts = [0] * (len(AGE_BUCKETS_S) + 1)
        self.age_sum = 0.0

    def observe_age(self, age_s: float) -> None:
        if age_s < 0.0:   # producer clock ahead of ours
            age_s = 0.0
        self.age_counts[bisect_left(AGE_BUCKETS_S, age_s)] += 1
        self.age_sum += age_s

    def histogram_lines(self, name: str) -> list[str]:
        lines = [f"# TYPE {name} histogram"]
        cumulative = 0
        for bound, n in zip(AGE_BUCKETS_S, self.age_counts):
            cumulative += n
            lines.append(f'{name}_bucket{{le="{bound}"}} {cumulative}')
        cumulative += self.age_counts[-1]
        lines += [f'{name}_bucket{{le="+Inf"}} {cumulative}', f"{name}_sum {self.age_sum:.3f}",
                  f"{name}_count {cumulative}"]
        return lines


# --- outputs ------------------------------------------------------------------------

class DeliveryFailures:
    """Producer delivery callback (only called for failures): the owner must not commit past them."""

    def __init__(self, metrics: KafkaMetrics):
        self.metrics = metrics
        self.failed = False
        self.lock = threading.Lock()

    def __call__(self, err: KafkaError | None, msg) -> None:
        if err is None:
            return
        with self.lock:
            self.metrics.produce_errors += 1
            self.failed = True
        if self.metrics.produce_errors <= 10 or self.metrics.produce_errors % 1000 == 0:
            log.error("delivery to %s failed (%d so far): %s", msg.topic(), self.metrics.produce_errors, err)


def make_producer(cfg: Config, client_id: str, failures: DeliveryFailures) -> Producer:
    conf = producer_config(cfg.kafka, client_id)
    conf["on_delivery"] = failures
    return Producer(conf)


def produce(producer: Producer, metrics: KafkaMetrics, topic: str, key: str, value: bytes | None,
            headers: list[tuple[str, bytes]] | None = None) -> None:
    """Produce, blocking while the local queue is full (backpressure instead of dropping)."""
    while True:
        try:
            if headers:
                producer.produce(topic, value, key, headers=headers)
            else:
                producer.produce(topic, value, key)
            return
        except BufferError:
            metrics.produce_backpressure += 1
            producer.poll(0.05)


class KafkaSink:
    """Engine outputs as Kafka records keyed by tunnel_id (no retained semantics)."""

    def __init__(self, cfg: Config, producer: Producer, metrics: KafkaMetrics, tracing=None):
        k = cfg.kafka
        self.producer = producer
        self.metrics = metrics
        self.tracing = tracing
        self.vehicles_topic = k.vehicles_topic
        self.traffic_topic = k.traffic_topic
        self.dropped = 0

    def vehicle(self, tunnel_id: str, payload: bytes, event: dict, dets: list[Detection]) -> None:
        headers = self.tracing.vehicle_headers(event, dets) if self.tracing is not None else None
        produce(self.producer, self.metrics, self.vehicles_topic, tunnel_id, payload, headers)
        self.metrics.observe_age(time.time() - event["ts_exit"] / 1000.0)

    def traffic(self, tunnel_id: str, payload: bytes) -> None:
        produce(self.producer, self.metrics, self.traffic_topic, tunnel_id, payload)


class MeteredSink:
    """Non-Kafka outputs in Kafka input mode (stdout / discard): still measure event age."""

    def __init__(self, inner, metrics: KafkaMetrics):
        self.inner = inner
        self.metrics = metrics
        self.dropped = 0

    def vehicle(self, tunnel_id: str, payload: bytes, event: dict, dets: list[Detection]) -> None:
        self.inner.vehicle(tunnel_id, payload, event, dets)
        self.metrics.observe_age(time.time() - event["ts_exit"] / 1000.0)

    def traffic(self, tunnel_id: str, payload: bytes) -> None:
        self.inner.traffic(tunnel_id, payload)


# --- geometry --------------------------------------------------------------------------

def geometry_value(tunnel_id: str, half: float, now_ms: int | None = None) -> bytes:
    return orjson.dumps({"tunnel_id": tunnel_id, "length_m": round(2 * half, 2),
                         "updated_ts": int(time.time() * 1000) if now_ms is None else now_ms})


def parse_geometry(key: bytes | None, value: bytes | None) -> tuple[str, float | None] | None:
    """(tunnel_id, half length or None for a tombstone); None if unusable."""
    if key is None:
        return None
    tunnel_id = key.decode("utf-8", "replace")
    if value is None:
        return tunnel_id, None
    try:
        length = float(orjson.loads(value)["length_m"])
    except (orjson.JSONDecodeError, KeyError, TypeError, ValueError):
        return None
    return (tunnel_id, length / 2) if length > 0 else None


class GeometryTopic:
    """Learned tunnel lengths in the compacted geometry topic (key tunnel_id).

    Reading uses assign() on every partition (no group membership, no commits); each
    read_to_end() continues where the previous one stopped, so the in-memory map always
    equals the topic folded up to its end at the time of the call."""

    def __init__(self, cfg: Config, client_id: str, producer: Producer, metrics: KafkaMetrics,
                 consumer_factory: Callable[[dict], Consumer] = Consumer):
        self.k = cfg.kafka
        self.topic = self.k.geometry_topic
        self.producer = producer
        self.metrics = metrics
        self.client_id = client_id
        self.consumer_factory = consumer_factory
        self.halves: dict[str, float] = {}        # shared with the engines (seeds new tunnels)
        self.published: dict[str, float] = {}     # length (rounded) last written to the topic
        self.consumer: Consumer | None = None
        self.positions: dict[int, int] = {}
        self.loaded = False
        self.records_read = 0
        self.records_published = 0

    def _open(self, timeout_s: float) -> None:
        conf = base_config(self.k, f"{self.client_id}-geometry")
        # Same group id as the main consumer (ACL), but never subscribes or commits.
        conf.update({"group.id": self.k.group_id, "enable.auto.commit": False, "enable.auto.offset.store": False,
                     "enable.partition.eof": True, "auto.offset.reset": "earliest"})
        consumer = self.consumer_factory(conf)
        md = consumer.list_topics(self.topic, timeout=timeout_s).topics.get(self.topic)
        if md is None or md.error is not None or not md.partitions:
            consumer.close()
            raise KafkaException(f"geometry topic {self.topic} not available: {md.error if md else 'missing'}")
        parts = sorted(md.partitions)
        consumer.assign([TopicPartition(self.topic, p, OFFSET_BEGINNING) for p in parts])
        self.positions = dict.fromkeys(parts, -1)
        self.consumer = consumer

    def read_to_end(self, timeout_s: float) -> int:
        """Apply every record up to the current end of each partition. Returns records applied."""
        deadline = time.monotonic() + timeout_s
        if self.consumer is None:
            self._open(timeout_s)
        consumer = self.consumer
        ends = {}
        for p in self.positions:
            low, high = consumer.get_watermark_offsets(TopicPartition(self.topic, p),
                                                       timeout=max(0.1, deadline - time.monotonic()))
            ends[p] = (low, high)
        applied = 0

        def done() -> bool:
            return all(low >= high or self.positions[p] >= high for p, (low, high) in ends.items())

        while not done():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"geometry topic {self.topic}: not at the end after {timeout_s:.0f}s "
                                   f"(positions {self.positions}, ends {ends})")
            for msg in consumer.consume(1000, min(0.5, remaining)):
                err = msg.error()
                p = msg.partition()
                if err is not None:
                    if err.code() == KafkaError._PARTITION_EOF:
                        self.positions[p] = max(self.positions.get(p, -1), msg.offset())
                        continue
                    raise KafkaException(err)
                self.positions[p] = msg.offset() + 1
                parsed = parse_geometry(msg.key(), msg.value())
                if parsed is None:
                    continue
                tid, half = parsed
                if half is None:
                    self.halves.pop(tid, None)
                    self.published.pop(tid, None)
                else:
                    self.halves[tid] = half
                    self.published[tid] = round(2 * half, 1)
                applied += 1
        self.records_read += applied
        self.loaded = True
        return applied

    def publish(self, tunnel_id: str, half: float) -> bool:
        """Write a tunnel's length if it changed (0.1 m resolution) since last written or read."""
        length = round(2 * half, 1)
        if self.published.get(tunnel_id) == length:
            return False
        produce(self.producer, self.metrics, self.topic, tunnel_id, geometry_value(tunnel_id, half))
        self.published[tunnel_id] = length
        self.halves[tunnel_id] = half
        self.records_published += 1
        return True

    def close(self) -> None:
        if self.consumer is not None:
            try:
                self.consumer.close()
            except (KafkaException, RuntimeError):
                pass
            self.consumer = None
