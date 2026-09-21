"""Kafka mode: one process, one consumer group member, one engine per assigned partition.

Tunnel ownership = the partitions of the detections topic assigned to this member
(cooperative-sticky). Records are keyed by tunnel_id with the murmur2 partitioner, so the
three sensors of a tunnel always reach the same partition and the same engine.

Delivery guarantee: at least once for vehicle events.
  * Commit, per partition, the low watermark: the offset of the oldest detection still pending
    in that partition's engine (its vehicle event is not emitted yet), else the next offset.
    The producer is flushed first and a delivery failure stops the process without committing.
  * The commit metadata records the position and event clock at commit time. A new owner of
    the partition replays from the watermark to that position without re-emitting what the
    previous owner already emitted (ConsensusEngine.begin_replay), so a rebalance produces
    few duplicates (same event_id) and no partial "orphan" events.
  * Revoked partitions: publish geometry, flush, commit, then drop the engine without emitting
    partial events. Shutdown does the same for every partition.
  * Learned geometry lives in the compacted geometry topic: read to its end at startup and
    before new partitions are taken over, written when a tunnel calibrates, on the save
    interval (owned tunnels whose length changed) and on revoke/shutdown.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time
from collections import Counter

import orjson
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from .config import Config
from .engine import IDLE_ADVANCE_S, ConsensusEngine, TopicSink
from .io import StdoutOutput
from .kafka_io import (
    DeliveryFailures,
    GeometryTopic,
    KafkaMetrics,
    KafkaSink,
    MeteredSink,
    consumer_config,
    make_producer,
)
from .status import COUNTERS, counter_lines, serve_http
from .tracing import Tracing, sampled_traceparent, tracing_enabled

log = logging.getLogger("tunnel_consensus.kafka")

LOOP_TICK_S = 0.1
LIVENESS_TIMEOUT_S = 120.0   # /healthz fails when the main loop has not turned for this long


def commit_metadata(pending_from: int, position: int, clock_s: float) -> str:
    return orjson.dumps({"w": pending_from, "p": position, "c": int(clock_s * 1000)}).decode()


def parse_commit_metadata(metadata: str | None) -> tuple[int, int, float] | None:
    """(pending watermark, position, event clock s) recorded by the previous owner, or None."""
    if not metadata:
        return None
    try:
        m = orjson.loads(metadata)
        pending_from, position, clock_ms = m["w"], m["p"], m["c"]
    except (orjson.JSONDecodeError, KeyError, TypeError):
        return None
    if not (isinstance(pending_from, int) and isinstance(position, int) and isinstance(clock_ms, int | float)):
        return None
    if pending_from < 0 or position < pending_from:
        return None
    return pending_from, position, clock_ms / 1000.0


class KafkaService:
    def __init__(self, cfg: Config, *, consumer=None, producer=None, geometry=None, tracing=None,
                 hostname: str | None = None):
        k = cfg.kafka
        self.cfg = cfg
        self.k = k
        self.topic = k.detections_topic
        self.client_id = k.resolved_client_id(hostname)
        self.metrics = KafkaMetrics()
        self.failures = DeliveryFailures(self.metrics)
        self.producer = producer if producer is not None else make_producer(cfg, self.client_id, self.failures)
        self.tracing = tracing
        if cfg.output.sink == "kafka":
            self.sink = KafkaSink(cfg, self.producer, self.metrics, tracing)
        elif cfg.output.sink == "stdout":
            self.sink = MeteredSink(TopicSink(StdoutOutput().publish, cfg.output), self.metrics)
        else:
            self.sink = MeteredSink(TopicSink(lambda topic, payload, retain: None, cfg.output), self.metrics)
        self.geometry = geometry if geometry is not None else GeometryTopic(cfg, self.client_id, self.producer,
                                                                          self.metrics)
        self.consumer = consumer
        self.engines: dict[int, ConsensusEngine] = {}
        self.retired: Counter = Counter()
        self.calibrated: list[tuple[str, float]] = []
        self.last_committed: dict[int, tuple[int, str]] = {}
        self.rebalances = 0
        self.assigned_once = False
        self.commits = 0
        self.commit_failures = 0
        self.revoked_pending = 0      # pending detections dropped on revoke (re-read by the new owner)
        self.lost_partitions = 0
        self.consume_errors = 0
        self.fatal: str | None = None
        self.stopping = False
        self.heartbeat = time.monotonic()
        self.metrics_text = ""
        self._prev_rate: tuple[float, int, int] | None = None

    # --- engines ---------------------------------------------------------------

    def _new_engine(self, partition: int) -> ConsensusEngine:
        return ConsensusEngine(self.cfg, partition=partition, sink=self.sink, known_halves=self.geometry.halves,
                               track_offsets=True, on_calibrated=self._on_calibrated)

    def _on_calibrated(self, tunnel_id: str, half: float) -> None:
        self.calibrated.append((tunnel_id, half))

    def _retire(self, partition: int) -> None:
        eng = self.engines.pop(partition)
        snap = eng.snapshot()
        for key in COUNTERS:
            self.retired[key] += snap.get(key, 0)
        self.revoked_pending += eng.stats.pending
        self.last_committed.pop(partition, None)

    # --- rebalance callbacks (run inside consumer.consume) -----------------------

    def on_assign(self, consumer, partitions: list[TopicPartition]) -> None:
        self.heartbeat = time.monotonic()
        self.rebalances += 1
        new = sorted({tp.partition for tp in partitions if tp.partition not in self.engines})
        if new:
            try:
                n = self.geometry.read_to_end(self.k.geometry_load_timeout_s)
                log.info("geometry topic read to end (%d new records, %d tunnels known)", n, len(self.geometry.halves))
            except Exception as e:  # keep the rebalance moving; worst case these tunnels recalibrate
                log.warning("geometry topic read failed, new tunnels may recalibrate: %s", e)
            committed: dict[int, TopicPartition] = {}
            try:
                for tp in consumer.committed([TopicPartition(self.topic, p) for p in new],
                                             timeout=self.k.geometry_load_timeout_s):
                    committed[tp.partition] = tp
            except KafkaException as e:  # no replay info: duplicates possible, never losses
                log.warning("could not fetch committed offsets for %s: %s", new, e)
            for p in new:
                eng = self._new_engine(p)
                tp = committed.get(p)
                boundary = parse_commit_metadata(tp.metadata) if tp is not None and tp.offset >= 0 else None
                if boundary is not None and boundary[1] > tp.offset:
                    pending_from, position, clock_s = boundary
                    eng.begin_replay(position, clock_s, max(pending_from, tp.offset))
                    log.info("partition %d: resuming at offset %d, replaying to %d without re-emitting "
                             "(previous owner's oldest pending detection: %d)", p, tp.offset, position, pending_from)
                elif tp is not None and tp.offset >= 0:
                    log.info("partition %d: resuming at offset %d", p, tp.offset)
                self.engines[p] = eng
        self.assigned_once = True
        log.info("assigned %s: now %d partition(s) %s", new or "nothing new", len(self.engines), sorted(self.engines))

    def on_revoke(self, consumer, partitions: list[TopicPartition]) -> None:
        self.heartbeat = time.monotonic()
        ps = sorted({tp.partition for tp in partitions if tp.partition in self.engines})
        if not ps:
            return
        self._publish_calibrated()
        self._publish_geometry(ps)
        self._commit(ps, consumer)
        dropped = sum(self.engines[p].stats.pending for p in ps)
        for p in ps:
            self._retire(p)
        log.info("revoked %s (dropped %d pending detections, the new owner re-reads them): now %d partition(s)",
                 ps, dropped, len(self.engines))

    def on_lost(self, consumer, partitions: list[TopicPartition]) -> None:
        ps = sorted({tp.partition for tp in partitions if tp.partition in self.engines})
        self.lost_partitions += len(ps)
        self._publish_calibrated()
        self._publish_geometry(ps)
        for p in ps:
            self._retire(p)
        log.warning("lost partitions %s (session timed out): state dropped without commit", ps)

    # --- geometry and commits ---------------------------------------------------------

    def _publish_calibrated(self) -> None:
        if self.calibrated:
            for tid, half in self.calibrated:
                self.geometry.publish(tid, half)
            self.calibrated.clear()

    def _publish_geometry(self, partitions=None) -> int:
        n = 0
        for p in self.engines if partitions is None else partitions:
            eng = self.engines.get(p)
            if eng is not None:
                for tid, half in eng.learned_halves().items():
                    n += self.geometry.publish(tid, half)
        return n

    def _commit(self, partitions, consumer) -> bool:
        """Flush the producer, then commit the low watermark of each partition."""
        if self.fatal is not None:
            return False
        if self.failures.failed:
            self.fatal = "produce delivery failed"
            return False
        remaining = self.producer.flush(self.k.flush_timeout_s)
        self.heartbeat = time.monotonic()
        if remaining > 0:
            self.commit_failures += 1
            log.warning("producer flush timed out with %d records in flight: not committing", remaining)
            return False
        if self.failures.failed:
            self.fatal = "produce delivery failed"
            log.error("a produced record was not delivered: stopping without committing past it")
            return False
        offsets = {}
        for p in partitions:
            eng = self.engines.get(p)
            if eng is None or eng.next_offset < 0:
                continue
            pending_from, position, clock_s = eng.replay_boundary()
            start = eng.replay_start(pending_from)
            meta = commit_metadata(pending_from, position, clock_s)
            if self.last_committed.get(p) != (start, meta):
                offsets[p] = TopicPartition(self.topic, p, start, metadata=meta)
        if not offsets:
            return True
        try:
            result = consumer.commit(offsets=list(offsets.values()), asynchronous=False)
        except KafkaException as e:
            self.commit_failures += 1
            log.warning("commit failed: %s", e)
            return False
        ok = True
        for tp in result or []:
            if tp.error is not None:
                ok = False
                log.warning("commit of partition %d failed: %s", tp.partition, tp.error)
            elif tp.partition in offsets:
                sent = offsets[tp.partition]
                self.last_committed[tp.partition] = (sent.offset, sent.metadata)
        self.commits += 1
        if not ok:
            self.commit_failures += 1
        return ok

    # --- periodic work -------------------------------------------------------------------

    def _caught_up(self, consumer, partition: int, eng: ConsensusEngine) -> bool:
        try:
            _, high = consumer.get_watermark_offsets(TopicPartition(self.topic, partition), cached=True)
        except KafkaException:
            return False
        return high >= 0 and eng.next_offset >= high

    def _tick(self, consumer, now: float) -> None:
        for p, eng in self.engines.items():
            if eng.next_offset < 0 or now - eng.last_input_wall <= IDLE_ADVANCE_S:
                continue
            # Idle and at the end of the partition: advance event time on wall time.
            if self._caught_up(consumer, p, eng):
                if eng.replay_until is not None:
                    eng.end_replay()
                eng.tick(now)

    def lag(self, consumer) -> tuple[int, int]:
        """(records behind the partition ends, records a handover would replay)."""
        behind = uncommitted = 0
        for p, eng in self.engines.items():
            if eng.next_offset < 0:
                continue
            try:
                _, high = consumer.get_watermark_offsets(TopicPartition(self.topic, p), cached=True)
            except KafkaException:
                high = -1
            if high >= 0:
                behind += max(0, high - eng.next_offset)
            uncommitted += eng.next_offset - eng.replay_start(eng.replay_boundary()[0])
        return behind, uncommitted

    def totals(self) -> dict:
        t = Counter(self.retired)
        pending = tunnels = calibrated = 0
        for eng in self.engines.values():
            snap = eng.snapshot()
            for key in COUNTERS:
                t[key] += snap.get(key, 0)
            pending += snap["pending"]
            tunnels += snap["tunnels"]
            calibrated += snap["tunnels_calibrated"]
        t.update(pending=pending, tunnels=tunnels, tunnels_calibrated=calibrated, dropped=self.metrics.produce_errors,
                 out_pending=len(self.producer))
        return t

    def ready(self) -> bool:
        return self.assigned_once and self.geometry.loaded and not self.stopping and self.fatal is None

    def healthy(self) -> bool:
        return time.monotonic() - self.heartbeat < LIVENESS_TIMEOUT_S and self.fatal is None

    def refresh_stats(self, consumer, log_line: bool) -> None:
        t = self.totals()
        behind, uncommitted = self.lag(consumer) if consumer is not None else (0, 0)
        replaying = sum(1 for e in self.engines.values() if e.replay_until is not None)
        m = self.metrics
        lines = counter_lines(t)
        gauges = {
            "ready": int(self.ready()),
            "kafka_assigned_partitions": len(self.engines),
            "kafka_replaying_partitions": replaying,
            "kafka_lag_records": behind,
            "kafka_uncommitted_records": uncommitted,
            "kafka_producer_queue": len(self.producer),
            "geometry_loaded_tunnels": len(self.geometry.halves),
        }
        counters = {
            "kafka_rebalances": self.rebalances,
            "kafka_commits": self.commits,
            "kafka_commit_failures": self.commit_failures,
            "kafka_produce_errors": m.produce_errors,
            "kafka_produce_backpressure": m.produce_backpressure,
            "kafka_consume_errors": self.consume_errors,
            "kafka_lost_partitions": self.lost_partitions,
            "kafka_revoked_pending": self.revoked_pending,
            "geometry_records_read": self.geometry.records_read,
            "geometry_records_published": self.geometry.records_published,
        }
        for name, v in gauges.items():
            lines += [f"# TYPE consensus_{name} gauge", f"consensus_{name} {v}"]
        for name, v in counters.items():
            lines += [f"# TYPE consensus_{name}_total counter", f"consensus_{name}_total {v}"]
        lines += m.histogram_lines("consensus_event_age_seconds")
        self.metrics_text = "\n".join(lines) + "\n"
        if not log_line:
            return
        now = time.monotonic()
        rate_in = rate_out = 0.0
        if self._prev_rate is not None and now > self._prev_rate[0]:
            dt = now - self._prev_rate[0]
            rate_in = (t["received"] - self._prev_rate[1]) / dt
            rate_out = (t["vehicles"] - self._prev_rate[2]) / dt
        self._prev_rate = (now, t["received"], t["vehicles"])
        age_n = sum(m.age_counts)
        log.info(
            "in=%s rec/s vehicles=%s/s | received=%s dup=%s lost=%s invalid=%s calibrating=%s | vehicles=%s "
            "(3 sensors %s, 2: %s, 1: %s) pending=%s | tunnels=%s calibrated=%s geometry=%s | partitions=%d "
            "replaying=%d suppressed=%s lag=%s uncommitted=%s rebalances=%d commits=%d failed=%d | out queue=%d "
            "errors=%d age avg=%.2fs ready=%s",
            f"{rate_in:,.0f}", f"{rate_out:,.0f}", f"{t['received']:,}", f"{t['duplicates']:,}", f"{t['lost']:,}",
            f"{t['invalid']:,}", f"{t['calibrating']:,}", f"{t['vehicles']:,}", f"{t['vehicles_3']:,}",
            f"{t['vehicles_2']:,}", f"{t['vehicles_1']:,}", f"{t['pending']:,}", f"{t['tunnels']:,}",
            f"{t['tunnels_calibrated']:,}", f"{len(self.geometry.halves):,}", len(self.engines), replaying,
            f"{t['replay_suppressed']:,}", f"{behind:,}", f"{uncommitted:,}", self.rebalances, self.commits,
            self.commit_failures, len(self.producer), m.produce_errors, m.age_sum / age_n if age_n else 0.0,
            self.ready(),
        )

    # --- main loop --------------------------------------------------------------------

    def _on_consume_error(self, err: KafkaError) -> None:
        self.consume_errors += 1
        if err.fatal():
            self.fatal = f"consumer: {err}"
            log.error("fatal consumer error: %s", err)
        elif self.consume_errors <= 10 or self.consume_errors % 1000 == 0:
            log.warning("consume error (%d so far): %s", self.consume_errors, err)

    def run(self, stop: threading.Event) -> int:
        cfg, k = self.cfg, self.k
        h = cfg.reporting
        try:
            self.geometry.read_to_end(k.geometry_load_timeout_s)
        except Exception as e:
            log.error("cannot read geometry topic %s: %s", k.geometry_topic, e)
            return 1
        log.info("geometry: %d tunnels known from %s", len(self.geometry.halves), k.geometry_topic)
        if self.consumer is None:
            self.consumer = Consumer(consumer_config(k, self.client_id))
        consumer = self.consumer
        consumer.subscribe([self.topic], on_assign=self.on_assign, on_revoke=self.on_revoke, on_lost=self.on_lost)
        log.info("consumer %s joined group %s on %s (sink=%s)", self.client_id, k.group_id, self.topic, cfg.output.sink)

        engines = self.engines
        consume, poll = consumer.consume, self.producer.poll
        batch = k.poll_batch
        tracing = self.tracing
        monotonic, time_ns = time.monotonic, time.time_ns
        now = monotonic()
        next_tick = now + LOOP_TICK_S
        next_stats = now + cfg.service.stats_interval_s
        next_save = now + cfg.geometry.save_interval_s
        next_commit = now + k.commit_interval_s
        last_health = now
        try:
            while not stop.is_set() and self.fatal is None:
                msgs = consume(batch, LOOP_TICK_S)
                now = monotonic()
                self.heartbeat = now
                if tracing is None:
                    for msg in msgs:
                        if msg.error() is not None:
                            self._on_consume_error(msg.error())
                            continue
                        eng = engines.get(msg.partition())
                        if eng is not None:
                            eng.ingest(msg.value(), now, msg.offset())
                else:
                    for msg in msgs:
                        if msg.error() is not None:
                            self._on_consume_error(msg.error())
                            continue
                        eng = engines.get(msg.partition())
                        if eng is None:
                            continue
                        tracing.record_start_ns = time_ns()
                        eng.ingest(msg.value(), now, msg.offset(), sampled_traceparent(msg.headers()))
                poll(0)
                if self.calibrated:
                    self._publish_calibrated()
                if now < next_tick:
                    continue
                next_tick = now + LOOP_TICK_S
                self._tick(consumer, now)
                if h.interval_s is not None and now - last_health >= h.interval_s:
                    for eng in engines.values():
                        if eng.replay_until is None:   # the previous owner reported the replayed part
                            eng.report_traffic(now - last_health)
                    last_health = now
                if now >= next_save:
                    self._publish_geometry()
                    next_save = now + cfg.geometry.save_interval_s
                if now >= next_commit:
                    self._commit(list(engines), consumer)
                    next_commit = monotonic() + k.commit_interval_s
                if now >= next_stats:
                    self.refresh_stats(consumer, log_line=True)
                    next_stats = now + cfg.service.stats_interval_s
        finally:
            self.shutdown()
        if self.fatal is not None:
            log.error("stopped: %s", self.fatal)
            return 1
        return 0

    def shutdown(self) -> None:
        """Stop consuming; no partial events: publish geometry, flush, commit watermarks, leave the group."""
        self.stopping = True
        consumer = self.consumer
        ps = sorted(self.engines)
        if ps:
            self._publish_calibrated()
            self._publish_geometry(ps)
            if consumer is not None and self.fatal is None:
                self._commit(ps, consumer)
            for p in ps:
                self._retire(p)
        self.refresh_stats(consumer, log_line=True)
        if consumer is not None:
            try:
                consumer.close()   # leaves the group: the remaining members take the partitions over
            except (KafkaException, RuntimeError) as e:
                log.warning("consumer close: %s", e)
        remaining = self.producer.flush(self.k.flush_timeout_s)
        if remaining:
            log.warning("%d records not delivered at shutdown", remaining)
        self.geometry.close()
        if self.tracing is not None:
            self.tracing.shutdown()
        log.info("stopped")


def run_kafka(cfg: Config) -> int:
    logging.basicConfig(level=cfg.logging.level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    stop = threading.Event()

    def request_stop(signum, _frame):
        if not stop.is_set():
            log.info("received %s, shutting down", signal.Signals(signum).name)
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    try:
        tracing = Tracing(cfg.kafka.vehicles_topic) if tracing_enabled() else None
        service = KafkaService(cfg, tracing=tracing)
    except (ValueError, KafkaException) as e:
        print(f"invalid kafka configuration: {e}", file=sys.stderr)
        return 2
    k = cfg.kafka
    log.info("kafka mode: bootstrap=%s protocol=%s group=%s input=%s sink=%s tracing=%s",
             k.bootstrap_servers, k.security_protocol, k.group_id, k.detections_topic, cfg.output.sink,
             tracing is not None)
    server = None
    if cfg.service.http_port:
        server = serve_http(cfg.service.http_port, service.healthy, service.ready, lambda: service.metrics_text)
    try:
        service.refresh_stats(None, log_line=False)
        return service.run(stop)
    finally:
        if server is not None:
            server.shutdown()
