"""Consume -> validate -> produce loop with at-least-once delivery.

Offsets are committed manually. A poll batch's offsets become committable only after Kafka has
acknowledged every record produced from it (detections and rejections), and batches are committed
in poll order, so a committed offset never skips an unacknowledged record. On revoke the producer
is flushed and offsets are committed before the partitions go away; on SIGTERM polling stops, the
producer is flushed, offsets are committed and the consumer leaves the group. A record Kafka does
not acknowledge stops the service without committing it (the restarted pod reprocesses it).
"""

from __future__ import annotations

import logging
import time
from bisect import bisect_left
from collections import deque
from collections.abc import Callable
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition

from .config import Config, KafkaConfig
from .metrics import Stats
from .normalize import DETECTION, HEALTH, process, reject_value
from .tracing import Tracing, passthrough_headers

log = logging.getLogger(__name__)

TOPIC_CHECK_RETRY_S = 5.0


def _client_config(k: KafkaConfig) -> dict[str, Any]:
    conf: dict[str, Any] = {
        "bootstrap.servers": k.bootstrap_servers,
        "security.protocol": k.security_protocol,
        "client.id": k.resolved_client_id(),
    }
    if k.security_protocol.startswith("SASL_"):
        conf.update({"sasl.mechanism": k.sasl_mechanism, "sasl.username": k.sasl_username,
                     "sasl.password": k.sasl_password})
    if k.ssl_ca_location:
        conf["ssl.ca.location"] = k.ssl_ca_location
    return conf


def consumer_config(k: KafkaConfig) -> dict[str, Any]:
    conf = _client_config(k)
    conf.update({
        "group.id": k.group_id,
        "partition.assignment.strategy": "cooperative-sticky",
        "enable.auto.commit": False,          # offsets are committed only after produce acks
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",      # a new group must not skip records already in the topic
        "queued.max.messages.kbytes": 16384,  # local prefetch cap (memory); default 64 MiB
    })
    conf.update(k.consumer)
    return conf


def producer_config(k: KafkaConfig) -> dict[str, Any]:
    conf = _client_config(k)
    conf.update({
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",
        "partitioner": "murmur2_random",      # Java client compatible: same key -> same partition
        "linger.ms": 5,
        "queue.buffering.max.kbytes": 65536,  # back-pressure cap (memory); default 1 GiB
    })
    conf.update(k.producer)
    return conf


class Batch:
    """One poll batch: its offsets are committable once every record produced from it is acknowledged."""

    __slots__ = ("offsets", "pending", "accepted", "rejected", "polled_at", "errors")

    def __init__(self, polled_at: float, errors: list):
        self.offsets: dict[int, int] = {}  # partition -> next offset to consume (last offset + 1)
        self.pending = 0                   # produced, not yet acknowledged
        self.accepted = 0
        self.rejected = 0
        self.polled_at = polled_at
        self.errors = errors               # shared: any delivery error stops the service

    def ack(self, err: Any, msg: Any) -> None:
        self.pending -= 1
        if err is not None:
            self.errors.append((self, err, msg))


class Service:
    def __init__(self, cfg: Config, stats: Stats, tracing: Tracing | None = None,
                 consumer_factory: Callable[[dict], Any] = Consumer,
                 producer_factory: Callable[[dict], Any] = Producer):
        self.cfg = cfg
        self.k = cfg.kafka
        self.stats = stats
        self.tracing = tracing
        kafka_log = logging.getLogger("librdkafka")
        self.consumer = consumer_factory(consumer_config(self.k) | {
            "error_cb": lambda err: self._on_client_error("consumer", err), "logger": kafka_log})
        self.producer = producer_factory(producer_config(self.k) | {
            "error_cb": lambda err: self._on_client_error("producer", err), "logger": kafka_log})
        self.batches: deque[Batch] = deque()   # in poll order
        self.delivery_errors: list = []
        self.uncommitted: dict[int, int] = {}  # acknowledged, not yet committed: partition -> offset
        self.committed: dict[int, int] = {}
        self.assigned: set[int] = set()
        self.joined = False                    # rebalance completed at least once (assignment may be empty)
        self.producer_ok = False               # topics checked; no all-brokers-down since
        self.stopping = False
        self.fatal: str | None = None
        self._last_report = (time.monotonic(), 0, 0, 0)

    # --- state for probes/metrics (read from the HTTP thread) ----------------------

    def ready(self) -> bool:
        return self.joined and self.producer_ok and not self.stopping and self.fatal is None

    def inflight_records(self) -> int:
        try:
            return len(self.producer)
        except Exception:  # noqa: BLE001 - probe must not fail
            return 0

    def request_stop(self, reason: str) -> None:
        if not self.stopping:
            log.info("%s: stopping (stop polling, flush, commit, leave group)", reason)
        self.stopping = True

    # --- main loop -----------------------------------------------------------------

    def run(self) -> int:
        k = self.k
        if not self._wait_for_topics():
            return self._shutdown()
        self.consumer.subscribe([k.input_topic], on_assign=self._on_assign, on_revoke=self._on_revoke,
                                on_lost=self._on_lost)
        log.info("consuming %s (group %s) -> %s / %s / %s", k.input_topic, k.group_id, k.output_topic,
                 k.health_topic, k.rejected_topic)
        consume, poll = self.consumer.consume, self.producer.poll
        now = time.monotonic()
        next_commit = now + k.commit_interval_s
        next_stats = now + self.cfg.service.stats_interval_s
        while not self.stopping and self.fatal is None:
            try:
                msgs = consume(k.poll_batch, k.poll_timeout_s)
            except KafkaException as e:
                self._set_fatal(f"consumer failed: {e}")
                break
            if msgs:
                self._process_batch(msgs)
            poll(0)  # delivery reports
            self._collect()
            now = time.monotonic()
            if now >= next_commit:
                self._commit()
                next_commit = now + k.commit_interval_s
            if now >= next_stats:
                if not self.producer_ok:
                    self._check_producer()
                self.report()
                next_stats = now + self.cfg.service.stats_interval_s
        return self._shutdown()

    def _process_batch(self, msgs: list) -> None:
        stats, tracing = self.stats, self.tracing
        started = time.perf_counter()
        now_ms = time.time_ns() // 1_000_000
        batch = Batch(started, self.delivery_errors)
        self.batches.append(batch)
        offsets, ack = batch.offsets, batch.ack
        produce = self.producer.produce
        output_topic, rejected_topic = self.k.output_topic, self.k.rejected_topic
        health_topic = self.k.health_topic
        rejected = stats.rejected
        age = stats.event_age
        age_bounds, age_counts = age.bounds, age.counts
        age_sum = 0.0
        consumed = 0
        for msg in msgs:
            if msg.error() is not None:
                self._on_consumer_error(msg)
                continue
            consumed += 1
            headers = msg.headers()
            if tracing is None:
                span = None
                out_headers = passthrough_headers(headers) if headers else None
            else:
                span, out_headers = tracing.begin(headers)
            kind, tunnel_id, record, info = process(msg.value(), now_ms)
            reason = None
            if kind is DETECTION:
                topic = output_topic
                batch.accepted += 1
                a = (now_ms - info["ts"]) / 1000.0
                if a < 0.0:
                    a = 0.0  # sensor clock ahead
                age_counts[bisect_left(age_bounds, a)] += 1
                age_sum += a
            elif kind is HEALTH:
                topic = health_topic
                batch.accepted += 1
                stats.health += 1
            else:
                reason = info
                topic = rejected_topic
                batch.rejected += 1
                rejected[reason] += 1
            on_delivery = ack
            if span is not None:
                stats.traced += 1
                out_headers = tracing.annotate(span, msg.partition(), msg.offset(), reason, topic, info)
                on_delivery = tracing.on_delivery(span, ack)
            batch.pending += 1
            try:
                produce(topic, record, tunnel_id, on_delivery=on_delivery, headers=out_headers)
            except BufferError:
                if not self._produce_when_space(batch, topic, record, tunnel_id, on_delivery, out_headers):
                    break
            except KafkaException as e:
                # Refused locally (e.g. record too large): reject it instead of blocking the partition.
                batch.pending -= 1
                if not self._produce_refused(batch, msg, reason, e, now_ms, ack):
                    break
            offsets[msg.partition()] = msg.offset() + 1
        stats.consumed += consumed
        age.sum += age_sum
        stats.batch_duration.observe(time.perf_counter() - started)

    def _produce_when_space(self, batch: Batch, topic: str, record: bytes, key: str | None, on_delivery: Any,
                            headers: Any) -> bool:
        """Producer queue full: serve delivery reports until the record fits (back-pressure)."""
        while True:
            self.producer.poll(0.1)
            try:
                self.producer.produce(topic, record, key, on_delivery=on_delivery, headers=headers)
                return True
            except BufferError:
                if self.stopping or self.fatal is not None:
                    batch.pending -= 1
                    log.warning("stopping while the producer queue is full: the rest of the batch stays uncommitted")
                    # Nothing after this record may be committed: a batch with an error is never committed.
                    self.delivery_errors.append((batch, "not produced: shutdown with full producer queue", None))
                    return False

    def _produce_refused(self, batch: Batch, msg: Any, reason: str | None, error: KafkaException, now_ms: int,
                         ack: Any) -> bool:
        if reason is not None:
            self._set_fatal(f"producer refused a rejection record: {error}")
            return False
        _, tunnel_id, record, reason = reject_value(msg.value(), now_ms, "bad_payload",
                                                    f"producer refused the normalized record: {error}")
        batch.accepted -= 1
        batch.rejected += 1
        self.stats.rejected[reason] += 1
        batch.pending += 1
        try:
            self.producer.produce(self.k.rejected_topic, record, tunnel_id, on_delivery=ack)
        except (BufferError, KafkaException) as e:
            batch.pending -= 1
            self._set_fatal(f"producer refused a rejection record: {e}")
            return False
        return True

    def _collect(self) -> None:
        """Move fully acknowledged batches (in poll order) to the uncommitted offsets."""
        if self.delivery_errors:
            self._on_delivery_errors()
        batches, stats = self.batches, self.stats
        now = time.perf_counter()
        while batches:
            b = batches[0]
            if b.pending or (self.delivery_errors and any(e[0] is b for e in self.delivery_errors)):
                return
            batches.popleft()
            self.uncommitted.update(b.offsets)
            stats.produced += b.accepted
            stats.rejected_produced += b.rejected
            stats.batch_ack.observe(now - b.polled_at)
            if not self.producer_ok and (b.accepted or b.rejected):
                self.producer_ok = True
                log.info("producer healthy again")

    def _on_delivery_errors(self) -> None:
        self.stats.delivery_failures = len(self.delivery_errors)
        if self.fatal is None:
            _, err, msg = self.delivery_errors[0]
            where = f" ({msg.topic()} key={msg.key()!r})" if msg is not None else ""
            self._set_fatal(f"record not acknowledged{where}: {err}")

    def _commit(self) -> None:
        offsets = {p: o for p, o in self.uncommitted.items() if p in self.assigned}
        self.uncommitted = {}
        if not offsets:
            return
        topic = self.k.input_topic
        try:
            result = self.consumer.commit(offsets=[TopicPartition(topic, p, o) for p, o in offsets.items()],
                                          asynchronous=False)
        except KafkaException as e:
            self.stats.commit_failures += 1
            log.warning("offset commit failed, retrying with the next commit: %s", e)
            self._requeue(offsets)
            return
        failed = {}
        for tp in result or ():
            if tp.error is not None:
                failed[tp.partition] = offsets[tp.partition]
            else:
                self.committed[tp.partition] = tp.offset
        if failed:
            self.stats.commit_failures += 1
            log.warning("offset commit failed for partitions %s, retrying with the next commit", sorted(failed))
            self._requeue(failed)
        else:
            self.stats.commits += 1

    def _requeue(self, offsets: dict[int, int]) -> None:
        for p, o in offsets.items():
            if p in self.assigned and p not in self.uncommitted:  # a newer offset supersedes a failed one
                self.uncommitted[p] = o

    # --- rebalance callbacks (run inside consume()/close()) --------------------------

    def _on_assign(self, _consumer: Any, partitions: list) -> None:
        added = {tp.partition for tp in partitions}
        self.assigned |= added
        self.joined = True
        self.stats.rebalances += 1
        log.info("assigned partitions %s -> now %s", sorted(added), sorted(self.assigned))

    def _on_revoke(self, _consumer: Any, partitions: list) -> None:
        revoked = {tp.partition for tp in partitions}
        self.stats.rebalances += 1
        # Finish what was produced so the next owner starts after it (fewer duplicates).
        remaining = self.producer.flush(0 if self.stopping else self.k.shutdown_timeout_s)
        self._collect()
        self._commit()
        if remaining:
            log.warning("%d records still unacknowledged at revoke: their offsets stay uncommitted", remaining)
        committed = {p: self.committed[p] for p in sorted(revoked) if p in self.committed}
        self._forget(revoked)
        log.info("revoked partitions %s (committed offsets %s) -> now %s", sorted(revoked), committed,
                 sorted(self.assigned))

    def _on_lost(self, _consumer: Any, partitions: list) -> None:
        lost = {tp.partition for tp in partitions}
        self.stats.rebalances += 1
        self._forget(lost)
        if not self.assigned:
            self.joined = False
        log.warning("lost partitions %s (not committed, the new owner reprocesses them)", sorted(lost))

    def _forget(self, partitions: set[int]) -> None:
        for b in self.batches:
            for p in partitions:
                b.offsets.pop(p, None)
        for p in partitions:
            self.uncommitted.pop(p, None)
            self.committed.pop(p, None)
        self.assigned -= partitions

    # --- errors, startup, shutdown ----------------------------------------------------

    def _on_client_error(self, client: str, err: KafkaError) -> None:
        if err.fatal():
            self._set_fatal(f"{client} fatal error: {err}")
        elif err.code() == KafkaError._ALL_BROKERS_DOWN:
            if client == "producer":
                self.producer_ok = False
            log.warning("%s: %s", client, err)
        else:
            log.warning("%s: %s", client, err)

    def _on_consumer_error(self, msg: Any) -> None:
        err = msg.error()
        if err.code() == KafkaError._PARTITION_EOF:
            return
        self.stats.consumer_errors += 1
        if err.fatal():
            self._set_fatal(f"consumer fatal error: {err}")
        else:
            log.warning("consumer: %s", err)

    def _set_fatal(self, reason: str) -> None:
        if self.fatal is None:
            log.error("fatal: %s", reason)
            self.fatal = reason

    def _missing_topics(self) -> list[str]:
        k = self.k
        missing = []
        for client, topic in ((self.consumer, k.input_topic), (self.producer, k.output_topic),
                              (self.producer, k.health_topic), (self.producer, k.rejected_topic)):
            try:
                md = client.list_topics(topic=topic, timeout=5)
                t = md.topics.get(topic)
                if t is None or t.error is not None:
                    missing.append(f"{topic} ({t.error if t is not None else 'unknown'})")
            except KafkaException as e:
                missing.append(f"{topic} ({e})")
        return missing

    def _wait_for_topics(self) -> bool:
        """Block until the three topics are visible (broker reachable, credentials and ACLs OK)."""
        while not self.stopping and self.fatal is None:
            missing = self._missing_topics()
            if not missing:
                self.producer_ok = True
                return True
            log.warning("waiting for Kafka topics: %s", ", ".join(missing))
            deadline = time.monotonic() + TOPIC_CHECK_RETRY_S
            while time.monotonic() < deadline and not self.stopping:
                time.sleep(0.1)
        return False

    def _check_producer(self) -> None:
        try:
            md = self.producer.list_topics(topic=self.k.output_topic, timeout=2)
            t = md.topics.get(self.k.output_topic)
            if t is not None and t.error is None:
                self.producer_ok = True
                log.info("producer healthy again")
        except KafkaException:
            pass

    def _shutdown(self) -> int:
        remaining = self.producer.flush(self.k.shutdown_timeout_s)
        self._collect()
        self._commit()
        committed = dict(sorted(self.committed.items()))
        try:
            self.consumer.close()  # leaves the group; on_revoke commits anything left
        except (KafkaException, RuntimeError) as e:
            log.warning("consumer close: %s", e)
        self.report()
        if remaining:
            log.warning("%d records were not acknowledged: their offsets stay uncommitted", remaining)
        if self.fatal is not None:
            log.error("stopped after fatal error: %s", self.fatal)
            return 1
        log.info("stopped cleanly: committed offsets %s", committed)
        return 0

    def report(self) -> None:
        s = self.stats
        now = time.monotonic()
        rejected = s.rejected_total()
        t0, consumed0, produced0, rejected0 = self._last_report
        dt = max(now - t0, 1e-9)
        self._last_report = (now, s.consumed, s.produced, rejected)
        reasons = " ".join(f"{r}={n:,}" for r, n in s.rejected.items() if n)
        log.info(
            "in=%s msg/s out=%s/s rejected=%s/s | consumed=%s produced=%s rejected=%s%s | "
            "inflight=%s batches=%s partitions=%s commits=%s commit_failures=%s | ready=%s",
            f"{(s.consumed - consumed0) / dt:,.0f}", f"{(s.produced - produced0) / dt:,.0f}",
            f"{(rejected - rejected0) / dt:,.0f}", f"{s.consumed:,}", f"{s.produced:,}", f"{rejected:,}",
            f" ({reasons})" if reasons else "", f"{self.inflight_records():,}", len(self.batches),
            len(self.assigned), f"{s.commits:,}", f"{s.commit_failures:,}", self.ready(),
        )

